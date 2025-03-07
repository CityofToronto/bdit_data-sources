import os
import datetime
import dateutil
import pytz
import json
from requests import Session
import logging
import pandas as pd
import numpy as np
import click
from psycopg2 import sql
from psycopg2.extras import execute_values

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

def logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter=logging.Formatter('%(asctime)s     	%(levelname)s    %(message)s', datefmt='%d %b %Y %H:%M:%S')
    stream_handler=logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger

logger = logger()
logger.debug('Start')

session = Session()
session.proxies = {}
URL_BASE = 'https://api.miovision.one/api/v1'
TZ = pytz.timezone("Canada/Eastern")
time_delta = datetime.timedelta(days=1)
default_start = str(datetime.date.today()-time_delta)
default_end = str(datetime.date.today())

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), 'sql')

CONTEXT_SETTINGS = dict(
    default_map={'run_alerts_api_cli': {'flag': 0}}
)

def run_alerts_api(start_date: str, end_date: str):
    api_key = BaseHook.get_connection('miovision_api_key')
    key = api_key.extra_dejson['key']
    mio_postgres = PostgresHook("miovision_api_bot")
    start_date = dateutil.parser.parse(str(start_date))
    end_date = dateutil.parser.parse(str(end_date))
    with mio_postgres.get_conn() as conn:
        pull_alerts(conn, start_date, end_date, key)

@click.group(context_settings=CONTEXT_SETTINGS)
def cli():
    pass

@cli.command()
@click.option('--start_date', default=default_start, help='format is YYYY-MM-DD for start date')
@click.option('--end_date' , default=default_end, help='format is YYYY-MM-DD for end date & excluding the day itself')

def run_alerts_api_cli(start_date, end_date):
    return run_alerts_api(start_date, end_date)

class MiovAlertPuller:
    """Miovision API puller.

    Basic workflow is to initialize the class, then use `get_response` to
    pull alerts all intersections at a point in time.

    Parameters
    ----------
    active_time : str
        Alert active time in yyyy-mm-dd hh:mm:ss
    key : str
        Miovision API access key.
    """
    headers = {'Content-Type': 'application/json',
               'apikey': ''}
    def __init__(self, url, start_time, end_time, key):
        self.url = url
        self.start_time = start_time.isoformat()
        self.end_time = end_time.isoformat()
        self.headers['apikey'] = key
    def get_response(self):
        """Requests data from API."""
        params = {'startDateTime': self.start_time,
                  'endDateTime': self.end_time}
        response = session.get(
            url=self.url,
            params=params,
            headers=self.headers
        )
        # Return if we get a success response code, or raise an error if not.
        if response.status_code == 200:
            return response
        elif response.status_code == 401:
            raise Exception('Error' + str(response.status_code))
        elif response.status_code == 404:
            raise Exception('Error' + str(response.status_code))
    def process_timestamp(self, utc_timestamp):
        if utc_timestamp is None:
            return None
        utc_timestamp = dateutil.parser.parse(str(utc_timestamp))
        #convert timestamp from utc to local TZ
        local_timestamp = utc_timestamp.replace(tzinfo=pytz.utc).astimezone(TZ)
        return local_timestamp.replace(tzinfo=None)
    def process_alert(self, row):
        """Process one row of Alert API output."""
        # Return time, classification_uid, leg, movement, volume.
        start_timestamp = self.process_timestamp(row['alertStartDateTime'])
        end_timestamp = self.process_timestamp(row['alertEndDateTime'])
        return (row['alertId'], start_timestamp, end_timestamp, row['intersectionId'], row['type'])
    def process_response(self, response):
        """Process the output of self.get_response."""
        data = json.loads(response.content.decode('utf-8'))
        return [self.process_alert(row) for row in data['alerts']], data['links']['next']

def pull_alerts(conn: any, start_date: datetime, end_date: datetime, key: str):
    """Miovision Alert Puller

    Basic workflow is to initialize the class, then use `get_response` to
    pull alerts all intersections at a point in time, tranform and insert.

    Parameters
    ----------
    conn : any
        Postgres connection.
    start_date : datetime
        Start date to pull alerts from, inclusive.
    end_date : datetime
        End date to pull alerts to, inclusive.
    key : str
        Miovision API access key.
    """
    start_date = TZ.localize(start_date)
    end_date = TZ.localize(end_date)
    if end_date < start_date:
        raise ValueError('end_time is not greater than start_time.')
    logger.info('Pulling Miovision alerts from %s to %s.', start_date, end_date)
    #pull alerts from each page and append to list
    dfs = []
    pageSize = 100
    url = f"{URL_BASE}/alerts?pageSize={pageSize}&pageNumber=0"
    while url is not None: 
        miovpull = MiovAlertPuller(url, start_date, end_date, key)
        response = miovpull.get_response()
        alerts, url = miovpull.process_response(response)
        df = pd.DataFrame(alerts)
        dfs.append(df)
    logger.info('Done pulling. Transforming alerts.')
    #create pandas df and restructure
    final = pd.concat(dfs, ignore_index=True)
    final.rename(columns={0: "alertId", 1: "alertStartDateTime", 2: "alertEndDateTime", 3: "intersection_id", 4: "type"}, inplace = True)
    final.replace({np.NaN: None}, inplace = True)
    #convert to tuples for inserting
    values = list(final.itertuples(index=False, name=None))

    #sql insert data script
    fpath = os.path.join(SQL_DIR, 'inserts/insert-miovision_alerts.sql')
    with open(fpath, 'r', encoding='utf-8') as file:
        insert_query = sql.SQL(file.read())

    logger.info('Inserting values into `miovision_api.alerts`.')
    with conn.cursor() as cur:
        execute_values(cur, insert_query, values)

if __name__ == '__main__':
    cli()