import requests
import logging
from typing import Callable
from configparser import ConfigParser
from psycopg import connect
from datetime import datetime, timedelta
from airflow.sdk.exceptions import AirflowFailException

LOGGER = logging.getLogger(__name__)

default_start = datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0)-timedelta(days=1)
default_end = datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0)

URL = 'https://api.eco-counter.com/api/v2/'

# get a list of all sites from the API
def getSites(password_getter: Callable[[], str], sites: any = ()):
    pw = password_getter() # Retrieve only when needed
    response = requests.get(
        f'{URL}/sites',
        headers={'X-API-KEY': f'{pw}'},
        params={
            'include': 'flows',
            'pageSize': 500
        }
    )
    if response.status_code!=200:
        raise AirflowFailException(f"{response.status_code}: {response.reason}")
    if sites == ():
        return response.json()
    
    #otherwise filter sites using optional param.
    result = []
    for site in response.json():
        if site['id'] in sites:
            result.append(site)
    return result

# get all of a site's data from the API
def getSiteData(password_getter: Callable[[], str], site_id: int, startDate: datetime, endDate: datetime):
    pw = password_getter() # Retrieve only when needed
    requestChunkSize = timedelta(days=30)
    requestStart = startDate
    data = []
    while requestStart < endDate:
        requestEnd = min(requestStart + requestChunkSize, endDate)
        for _ in range(3):
            response = requests.get(
                f'{URL}/history/traffic/raw',
                headers={'X-API-KEY': pw},
                params={
                    'siteId': site_id,
                    'startDate': requestStart.strftime('%Y-%m-%d'),
                    'endDate':  requestEnd.strftime('%Y-%m-%d'),
                    'gapFilling': 'false',
                    'validatedDataOnly': 'false',
                    'rawDataOnly': 'true'
                }
            )
            if response.status_code==200:
                data += response.json()
                requestStart += requestChunkSize
                break
            elif response.status_code==429: #too many requests
                LOGGER.warning('Retrying in 2 minutes if tries remain.')
                time.sleep(120)
            elif response.status_code==401:
                raise AirflowFailException(f"{response.status_code}: {response.reason}")
            else:
                raise AirflowFailException(f"{response.status_code}: {response.reason}")
    return data

def getKnownSites(conn: any):
    with conn.cursor() as cur:
        cur.execute('SELECT site_id FROM ecocounter.sites_unfiltered WHERE date_decommissioned IS NULL;')
        sites = cur.fetchall()
        return [site[0] for site in sites]

# do we have a record of this site in the database?
def siteIsKnownToUs(site_id: int, conn: any):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM ecocounter.sites_unfiltered WHERE site_id = %s;",
            (site_id,)
        )
        return cursor.rowcount > 0

# do we have a record of this flow in the database?
def flowIsKnownToUs(flow_id: int, conn: any):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM ecocounter.flows_unfiltered WHERE flow_id = %s;",
            (flow_id,)
        )
        return cursor.rowcount > 0

# DANGER delete all count data for a given site
def truncateSiteSince(site_id: int, conn: any, startDate: datetime, endDate: datetime):
    with conn.cursor() as cursor:
        cursor.execute(
            """DELETE FROM ecocounter.counts_unfiltered AS c
            USING ecocounter.flows_unfiltered AS f
            WHERE
                f.site_id = %s
                AND c.flow_id = f.flow_id
                AND c.datetime_bin >= %s
                AND c.datetime_bin < %s""",
            (site_id, startDate, endDate)
        )

# insert records
def insertSiteCounts(conn: any, volume: any):
    insert_query="INSERT INTO ecocounter.counts_unfiltered (flow_id, datetime_bin, volume) VALUES (%s, %s, %s)"
    with conn.cursor() as cur:
        cur.executemany(insert_query, volume)
    return cur.query

# insert new site record
def insertSite(conn: any, site_id: int, site_name: str, lon: float, lat: float):
    insert_query="""
    INSERT INTO ecocounter.sites_unfiltered (site_id, site_description, geom, validated)
    VALUES (
        %s::numeric,
        %s::text,
        ST_SetSRID(ST_MakePoint(%s, %s), 4326),
        null::boolean --not validated by default
    )
    """
    with conn.cursor() as cur:
        cur.execute(insert_query, (site_id, site_name, lon, lat))

# insert new flow record
def insertFlow(conn: any, flow_id: int, site_id: int, flow_name: str, granularity: str):
    insert_query="""
    INSERT INTO ecocounter.flows_unfiltered (flow_id, site_id, flow_direction, bin_size, validated)
    VALUES (
        %s::numeric,
        %s::numeric,
        coalesce(lower(substring(%s::text, '(West|East|North|South)'))||'bound', 'unknown'),
        CASE %s WHEN 'PT1H' THEN '60 minutes'::interval WHEN 'PT15M' THEN '15 minutes'::interval ELSE NULL END,
        null::boolean --not validated
    )
    """
    with conn.cursor() as cur:
        cur.execute(insert_query, (flow_id, site_id, flow_name, granularity))

def truncate_and_insert(conn, token, site_id, start_date, end_date):
    LOGGER.info(f'Attempting to fetch data for site {site_id} from {start_date} to {end_date}.')
    # empty the count table for this flow
    truncateSiteSince(site_id, conn, start_date, end_date)
    # and fill it back up!
    counts = getSiteData(token, site_id, start_date, end_date)
    
    #convert response into a tuple for inserting
    for flow in counts:
        volume=[]
        for dt in flow['data']:
            row=(flow['flowID'], dt['timestamp'], dt['counts'])
            volume.append(row)
        #log and insert one flow at a time
        LOGGER.info(f"{len(volume)} rows fetched for flow {flow['flowID']} from {start_date} to {end_date}.")
        insertSiteCounts(conn, volume)

#for testing/pulling data without use of airflow.
def run_api(
        start_date: datetime = default_start,
        end_date: datetime = default_end,
        sites: any = ()
):
    CONFIG_PATH = '/data/airflow/data_scripts/bdit_data-sources/volumes/ecocounter/.api-v2-credentials.config'
    config = ConfigParser()
    config.read(CONFIG_PATH)
    conn = connect(**config['DBSETTINGS'])
    conn.autocommit = True
    password_getter = lambda: config['API']['password']
    for site in getSites(password_getter, sites=sites): #optionally specify site_ids here. 
        # only update data for sites / flows in the database
        # but announce unknowns for manual validation if necessary
        if not siteIsKnownToUs(site['id'], conn):
            print('unknown site', site['id'], site['name'])
            continue
        truncate_and_insert(conn, password_getter, site['id'], start_date, end_date)