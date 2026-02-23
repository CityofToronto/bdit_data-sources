'''Script that can be run manually to update `miovision_api.camera_details`.


'''
import pytz
import os
from requests import Session
import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime
import logging

from airflow.sdk.bases.hook import BaseHook

from .intersection_tmc import get_intersection_info

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'sql')

session = Session()
session.proxies = {}

def headers():
    '''get api key from airflow variable.'''
    api_key = BaseHook.get_connection('miovision_api_key')
    headers = {
        'Content-Type': 'application/json',
        'apikey': api_key.extra_dejson['key']
    }
    return headers

URL_BASE = "https://api.miovision.one/api/v1"

def get_cameras(conn):
    intersections = get_intersection_info(conn)
    cameras = pd.DataFrame()
    HEADERS=headers()
    #for each intersection, query it's camera details
    for intersection in intersections:
        response = session.get(
            URL_BASE + f"/intersections/{intersection.id1}/cameras",
            params={},
            headers=HEADERS,
            proxies=session.proxies
        )
        if response.status_code == 200:
            cameras_i = pd.DataFrame(response.json()['cameras'])
            if cameras_i.empty:
                continue
            cameras_i = cameras_i[['id', 'label']]
            cameras_i = cameras_i.add_prefix('camera_')
            cameras_i['intersection_id'] = intersection.id1
            cameras = pd.concat([cameras, cameras_i])
        else:
            #don't need to fail this non-critical pipeline
            LOGGER.info(f"Intersection {intersection.id1} recieved {response.status_code} error: {response.reason}")

    final = [tuple(x) for x in cameras.to_numpy()] #convert to tuples for inserting

    fpath = os.path.join(SQL_DIR, 'inserts/insert-camera_details.sql')
    with open(fpath, 'r', encoding='utf-8') as file:
        insert_query = sql.SQL(file.read())

    # Get intersections currently stored in `miovision_api` on Postgres.
    with conn.cursor() as cur:
        execute_values(cur, insert_query, final)
        
def get_configuration_dates(conn):
    intersections = get_intersection_info(conn)
    HEADERS=headers()
    configs = []
    for intersection in intersections:
        response = session.get(
            URL_BASE + f"/intersections/{intersection.id1}/hardware/detectionConfiguration",
            params={},
            headers=HEADERS,
            proxies=session.proxies
        )
        if response.status_code == 200:
            if response.json()['lastUpdated'] is not None:
                config_i = (
                    intersection.uid,
                    datetime.fromtimestamp(
                        response.json()['lastUpdated']/1000,
                        tz=pytz.timezone('America/Toronto')
                    )
                )
                configs.append(config_i)
        else:
            #don't need to fail this non-critical pipeline
            LOGGER.info(f"Intersection {intersection.id1} recieved {response.status_code} error: {response.reason}")
    sql='''INSERT INTO miovision_api.configuration_updates (intersection_uid, updated_time) VALUES %s
        ON CONFLICT (intersection_uid, updated_time) DO NOTHING'''
    with conn.cursor() as cur:
        execute_values(cur, sql, configs)