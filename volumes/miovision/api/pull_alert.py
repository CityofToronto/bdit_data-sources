import datetime
import json
from requests import Session
import logging
import configparser
import pandas as pd
from psycopg2 import connect
from psycopg2.extras import execute_values

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
url = 'https://api.miovision.com/alerts/'

"""
CONFIG = configparser.ConfigParser()
CONFIG.read('/etc/airflow/data_scripts/volumes/miovision/api/config.cfg')
api_key=CONFIG['API']
key=api_key['key']
dbset = CONFIG['DBSETTINGS']
conn = connect(**dbset)
conn.autocommit = True
"""

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
               'Authorization': ''}
    
    def __init__(self, active_time, key):
        self.active_time = active_time
        self.headers['Authorization'] = key

    def get_response(self):
        """Requests data from API."""

        params = {'activeAtTime': self.active_time}

        response = session.get(
            url,
            params=params,
            headers=self.headers,
            proxies=session.proxies)

        # Return if we get a success response code, or raise an error if not.
        if response.status_code == 200:
            return response
        elif response.status_code == 401:
            raise Exception('Error' + str(response.status_code))
        
    def process_alert(self, row):
        """Process one row of Alert API output."""
        
        # Return time, classification_uid, leg, movement, volume.
        return (self.active_time, row['intersectionId'], row['type'])

    def process_response(self, response):
        """Process the output of self.get_response."""
        data = json.loads(response.content.decode('utf-8'))
        return [self.process_alert(row) for row in data]

#insert data script
insert_data = """WITH new_values AS (
    SELECT intersection_id::text, alert::text, start_time::timestamp, end_time::timestamp
    FROM (VALUES %s) AS new_values(intersection_id, alert, start_time, end_time)
),

--extend existing alerts
updated AS (
    UPDATE miovision_api.alerts
    SET end_time = new_values.end_time
    FROM new_values 
    WHERE
        alerts.intersection_id = new_values.intersection_id
        AND alerts.alert = new_values.alert
        --where old end = new start
        AND alerts.end_time = new_values.start_time
    --returns the new values used for updates (to be excluded from insert)
    RETURNING new_values.*
)

--insert new alerts (exclude updated)
INSERT INTO miovision_api.alerts (intersection_id, alert, start_time, end_time)
SELECT intersection_id, alert, start_time, end_time FROM new_values
EXCEPT
SELECT intersection_id, alert, start_time, end_time FROM updated
ON CONFLICT (intersection_id, alert, start_time) DO NOTHING;

--update foreign key referencing miovision_api.intersections
--handles new records as well as old records with null intersection_uid (newly added intersections)
UPDATE miovision_api.alerts
SET intersection_uid = i.intersection_uid
FROM miovision_api.intersections AS i
WHERE alerts.intersection_id = i.id
AND alerts.intersection_uid IS NULL;
"""

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

    STEP_SIZE = datetime.timedelta(minutes=5)

    #establish list of timestamps to iterate over
    if end_date < start_date:
        raise ValueError('end_time is not greater than start_time.')
    dt = start_date
    dt_list = [dt, ]
    while(dt < end_date):
        dt = dt + STEP_SIZE
        dt_list.append(dt)

    logger.info('Pulling Miovision alerts from %s to %s.', start_date, end_date)

    #pull alerts at each timestamps and append to list
    dfs = []
    for dt in dt_list: 
        miovpull = MiovAlertPuller(dt, key)
        test = miovpull.get_response()
        response = miovpull.process_response(test)
        df = pd.DataFrame(response).drop_duplicates()
        dfs.append(df)

    logger.info('Done pulling. Transforming alerts.')

    #create pandas df and restructure        
    final = pd.concat(dfs, ignore_index=True)
    final.rename(columns={0: "time", 1: "intersection_id", 2: "alert"}, inplace = True)
    final['time'] = pd.to_datetime(final['time'])
    final.sort_values(by=['intersection_id', 'alert', 'time'], inplace = True)
    final.reset_index(drop = True)
    final.drop_duplicates(inplace = True, ignore_index = True)

    #group by alert and time
    final['shifted'] = final.groupby(by = ['intersection_id', 'alert'])['time'].shift(1)
    final['time_lag'] = final['time'] - final['shifted']
    final['start_time'] = final['time']

    #iterate through and check if consecutive
    for index, row in final.iterrows():
        if (row['time_lag'] == pd.Timedelta(f"{STEP_SIZE} minutes")): #lag = step size
            final.at[index, 'start_time'] = final.at[index-1, 'start_time'] #assign previous start time

    #find start and end time of consecutive values 
    summary = final.groupby(by = ['intersection_id', 'alert', 'start_time']).agg({'time': ['max']})
    summary = summary.reset_index()

    #convert to tuples for inserting
    values = list(summary.itertuples(index=False, name=None))
    
    logger.info('Inserting values into `miovision_api.alerts`.')
    with conn.cursor() as cur:
        execute_values(cur, insert_data, values)