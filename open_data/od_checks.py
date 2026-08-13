

import json
import logging
import requests
import logging
import pendulum
import pandas as pd
import dateutil.parser
import configparser
import psycopg2

from airflow.exceptions import AirflowNotFoundException
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

BASE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

def get_usage_stats(conn):
    # file clicks are also available under a different non-datastore resource
    datastore_search_url = BASE_URL + "/api/3/action/datastore_search"
    last_month = pendulum.now().subtract(months=1)
    ids = get_ids(conn)
    page_urls = [f"open.toronto.ca/dataset/{id}/" for id in ids]
    p = {
        "resource_id": "3f5d6284-0e9f-43d3-979e-01cefcc92f72", #'Page Views and Time Based Metrics.csv',
        "filters": json.dumps({
            "Link Source -Page URL": page_urls,
            "Month": last_month.format('YMM')
        })
    }
    resource_search_data = requests.get(datastore_search_url, params = p).json()["result"]
    # Convert to DataFrame and clean
    df = pd.DataFrame(resource_search_data['records'])
    df["Id"] = df["Link Source -Page URL"].str.extract(r'open.toronto.ca/dataset/([^/]+)')
    df["Month"] = pd.to_datetime(df["Month"], format="%Y%m")

    #filter columns and sort
    df_sorted = df[["Id", "Month", "Sessions", "Users", "Views", "Avg Session Duration (Sec)"]]
    data_tuples = [tuple(x) for x in df_sorted.to_numpy()]
    
    sql = """
    INSERT INTO open_data.od_page_views (page_id, mnth, sessions, users, views, avg_session_duration)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT od_page_views_pkey
    DO NOTHING
    """
    
    with conn.cursor() as cur:
        cur.executemany(sql, data_tuples)

def get_file_clicks(conn):
    sql = "SELECT MAX(_id) FROM open_data.od_file_clicks"
    with conn.cursor() as cur:
        cur.execute(sql)
        max_id = cur.fetchone()[0]

    datastore_search_url = BASE_URL + "/api/3/action/datastore_search"
    records = []
    offset = 0
    while True:
        p = {
            "resource_id": 'b308378d-2ec9-40ea-9182-f94cb63d73f3', #File URL Clicks
            "limit": 1000,
            "offset": offset
        }
        result = requests.get(datastore_search_url, params = p).json()["result"]
        records.extend(result['records'])
        if offset >= result["total"]:
            break
        offset += 1000

    # Convert to DataFrame and clean
    df = pd.DataFrame(records)
    df = df[df["_id"] > max_id]
    df["Month"] = pd.to_datetime(df["Month"], format="%Y%m")
    data_tuples = [tuple(x) for x in df.to_numpy()]

    sql = """INSERT INTO open_data.od_file_clicks (_id, url, clicks, mnth, package_name, resource_name)
        VALUES (%s, %s, %s, %s, %s, %s)"""
    with conn.cursor() as cur:
        cur.executemany(sql, data_tuples)
    
def get_ids(conn):
    ids_sql = "SELECT page_id FROM open_data.od_pages"
    with conn.cursor() as cur:
        cur.execute(ids_sql)
        ids = [r[0] for r in cur.fetchall()]
    return ids

def get_api_usage(conn):
    sql = "SELECT MAX(_id) FROM open_data.od_api_usage"
    with conn.cursor() as cur:
        cur.execute(sql)
        max_id = cur.fetchone()[0]
    
    datastore_search_url = BASE_URL + "/api/3/action/datastore_search"
    records = []
    offset = 0
    while True:
        p = {
            "resource_id": 'a67a1fa2-7c16-4d25-9c70-aa1bcb341b39', #API Usage
            "limit": 1000,
            "offset": offset
        }
        result = requests.get(datastore_search_url, params = p).json()["result"]
        records.extend(result['records'])
        if offset >= result["total"]:
            break
        offset += 1000
    
    # Convert to DataFrame and clean
    df = pd.DataFrame(records)
    df = df[df["_id"] > max_id]
    data_tuples = [tuple(x) for x in df.to_numpy()]
    
    sql = """
    INSERT INTO open_data.od_api_usage (_id, uri, count, dt, resource_id)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT od_api_usage_pkey
    DO NOTHING
    """
    
    with conn.cursor() as cur:
        cur.executemany(sql, data_tuples)
            

def get_resources(conn):
    resources = []
    pages = []
    ids = get_ids(conn)
    for id in ids:
        p = {"id": id}
        package_show_url = BASE_URL + "/api/3/action/package_show"
        result = requests.get(package_show_url, params = p).json()["result"]
        refresh_rate = result["refresh_rate"]
        last_refreshed = dateutil.parser.parse(result['last_refreshed'])
        pages.append((id, refresh_rate, last_refreshed))
        for resource in result["resources"]:
            resources.append((id, resource["name"], resource["id"]))
            
    sql="""INSERT INTO open_data.od_resources (page_id, resource_name, resource_id)
    VALUES (%s, %s, %s)
    ON CONFLICT DO NOTHING;"""
    
    with conn.cursor() as cur:
        cur.executemany(sql, resources)
        
    sql="""INSERT INTO open_data.od_pages (page_id, refresh_rate, last_refreshed)
    VALUES (%s, %s, %s)
    ON CONFLICT ON CONSTRAINT od_pages_pkey
    DO UPDATE SET
        refresh_rate = EXCLUDED.refresh_rate,
        last_refreshed = EXCLUDED.last_refreshed;
    """
    
    with conn.cursor() as cur:
        cur.executemany(sql, pages)
    
def get_connection(path=None):
    """
    Returns (conn, key). Uses Airflow hooks if running in Airflow,
    otherwise falls back to a local config file.
    """
    try:
        postgres = PostgresHook("ref_bot")
        conn = postgres.get_conn()
        conn.autocommit = True
        return conn
    except AirflowNotFoundException:
        if path is None:
            raise ValueError("Must provide a config path when running locally.")
        CONFIG = configparser.ConfigParser()
        CONFIG.read(path)
        dbset = CONFIG['DBSETTINGS']
        conn = psycopg2.connect(**dbset)
        conn.autocommit = True
        return conn

conn = get_connection('/data/home/gwolofs/db.cfg')