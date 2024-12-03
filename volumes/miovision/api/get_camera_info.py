import configparser
from requests import Session
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# Get api key from airflow variable.
config = configparser.ConfigParser()
config.read('/data/airflow/data_scripts/volumes/miovision/api/config.cfg')
api_key=config['API']

session = Session()
session.proxies = {}

headers = {'Content-Type': 'application/json',
           'apikey': api_key['key']}

URL_BASE = "https://api.miovision.one/api/v1"

# Get intersections from Miovision API.
response = session.get(
    URL_BASE + "/intersections?pageSize=1000",
    params={},
    headers=headers,
    proxies=session.proxies
)
intersections = pd.DataFrame(response.json()['intersections'])
intersections = intersections.add_prefix('intersection_')
intersections = intersections[intersections['intersection_name'] != 'Testing Lab'] #exclude Testing Lab

#for each intersection, query it's camera details
cameras = pd.DataFrame()
for i in intersections['intersection_id']:
    response = session.get(
        URL_BASE + f"/intersections/{i}/cameras",
        params={},
        headers=headers,
        proxies=session.proxies
    )
    cameras_i = pd.DataFrame(response.json()['cameras'])
    cameras_i = cameras_i.add_prefix('camera_')
    cameras_i['intersection_id'] = i
    cameras = pd.concat([cameras, cameras_i])

final = pd.merge(intersections, cameras, on = 'intersection_id')
final = [tuple(x) for x in final.to_numpy()] #convert to tuples for inserting

truncate_query = sql.SQL("TRUNCATE miovision_api.camera_details;")
insert_query = sql.SQL("""
WITH camera_details (
    intersection_id, intersection_lat, intersection_long, intersection_name, intersection_customId, camera_id, camera_type, camera_label, camera_streamUrl
) AS (VALUES %s)
INSERT INTO miovision_api.camera_details
SELECT cd.*
FROM camera_details AS cd
LEFT JOIN miovision_api.intersections AS i ON cd.intersection_id = i.id
WHERE i.date_decommissioned IS NULL;""")

# Get intersections currently stored in `miovision_api` on Postgres.
dbset = config['DBSETTINGS']
with psycopg2.connect(**dbset) as con, con.cursor() as cur:
    cur.execute(truncate_query)
    execute_values(cur, insert_query, final)