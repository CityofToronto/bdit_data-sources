import configparser
import pathlib
from requests import Session
import pandas as pd
import psycopg2
import datetime
import numpy as np
import pytz

# Get api key from airflow variable.
config = configparser.ConfigParser()
config.read('/data/airflow/data_scripts/volumes/miovision/api/config.cfg')
api_key=config['API']

session = Session()
session.proxies = {}

headers = {'Content-Type': 'application/json',
           'apikey': api_key['key']}

URL_BASE = "https://api.miovision.one/api/v1"
TZ = pytz.timezone("Canada/Eastern")

# Get intersections from Miovision API.
response = session.get(
    URL_BASE + "/intersections?pageSize=1000",
    params={},
    headers=headers,
    proxies=session.proxies
)
df_api = pd.DataFrame(response.json()['intersections'])
df_api = df_api[['id', 'name']].copy()
df_api.columns = ['id', 'intersection_name']
df_api = df_api[df_api['intersection_name'] != 'Testing Lab'] #exclude Testing Lab

# Get intersections currently stored in `miovision_api` on Postgres.
dbset = config['DBSETTINGS']
with psycopg2.connect(**dbset) as conn:
    df_pg = pd.read_sql("SELECT * FROM miovision_api.intersections", con=conn)

# Join the two tables, and select intersections in the API and not in Postgres.
df_intersections = pd.merge(df_pg, df_api[['id', 'intersection_name']], how='outer',
                            left_on='id', right_on='id', suffixes=('', '_api'))
df_newints = df_intersections.loc[df_intersections['intersection_uid'].isna(), ['id', 'intersection_name_api']]
df_newints.index += 1

print(df_newints)

#last two months by default
df_newints['test_daterange_start'] = datetime.date.today() - datetime.timedelta(days=60)
df_newints['test_daterange_end'] = datetime.date.today() - datetime.timedelta(days=1)

#or set individual intersection test dates:
#df_newints.loc[59, 'test_daterange_start'] = '2020-12-20'
#df_newints.loc[59, 'test_daterange_end'] = '2020-12-23'

def get_response_length(intersection_id, start_time, end_time):
    start_time = TZ.localize(start_time).isoformat()
    end_time = TZ.localize(end_time).isoformat()
    params = {'endDateTime': end_time,
            'startDateTime': start_time}
    response = session.get(
        URL_BASE + f"/intersections/{intersection_id}/tmc",
        params=params,
        headers=headers,
        proxies=session.proxies
    )
    if response.status_code != 200:
        return -1
    return len(response.json())

def get_first_data_date(intersection_id, start_time, end_time):
    # Generate a sequence of dates.
    for ctime in pd.date_range(start_time, end=end_time, freq='D').to_pydatetime():
        # The API throws an error when we query same day data.
        if ctime.date() >= datetime.date.today():
            return np.nan
        # For each date, try downloading 00:00 - 00:15 data
        response_length = get_response_length(intersection_id, ctime, ctime + datetime.timedelta(minutes=15))
        # It's highly unlikely the first timestamp of available data is from midnight to 12:10 AM,
        # so set the actual activation date to the day before ctime.
        if response_length > 0:
            return ctime - datetime.timedelta(days=1)   
    return np.nan

first_date_of_data = []

for i, row in df_newints.iterrows():
    first_date_of_data.append(
        get_first_data_date(
            row['id'],
            row['test_daterange_start'],
            row['test_daterange_end']))

df_newints['activation_date'] = first_date_of_data

print(df_newints)