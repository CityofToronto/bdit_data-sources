import sys
import json
from requests import Session
from requests import exceptions
import datetime
import dateutil.parser
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import connect, Error
import logging
import configparser
from time import sleep
from collections import namedtuple
import pandas as pd

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

#time_delta = datetime.timedelta(days=1)
#default_start = str(datetime.date.today()-time_delta)
#default_end = str(datetime.date.today())

session = Session()
session.proxies = {}
url = 'https://api.miovision.com/alerts/'

CONTEXT_SETTINGS = dict(
    default_map={'run_api': {'flag': 0}}
)

#start_date {{ds}} --end_date {{tomorrow_ds}}
input_time = '2022-05-23 02:01:03'

#def run_api(active_time, path, pull, dupes):

CONFIG = configparser.ConfigParser()
#CONFIG.read(path)
CONFIG.read('/etc/airflow/data_scripts/volumes/miovision/api/config.cfg')
api_key=CONFIG['API']
key=api_key['key']
active_time = dateutil.parser.parse(str(input_time))
logger.info('Pulling Miovision alerts active at %s.' %(active_time))

"""dbset = CONFIG['DBSETTINGS']
conn = connect(**dbset)
conn.autocommit = True
logger.debug('Connected to DB')
try:
    pull_data(conn, active_time, path, pull, key, dupes)
except Exception as e:
    logger.critical(traceback.format_exc())
    sys.exit(1)
"""

class MiovPuller:
    """Miovision API puller.

    Basic workflow is to initialize the class, then use `get_intersection` to
    pull TMC and crosswalk data for an intersection over one period of time.

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

#revisit this function
#def pull_data(conn, start_time, end_time, path, pull, key, dupes):

hours = ['2022-05-23T{}:'.format(str(h).zfill(2)) for h in range(24)]
five_min = [[h + str(m).zfill(2) for m in range(0, 60, 5)] for h in hours]
flat_list = [item for sublist in five_min for item in sublist]

final = pd.DataFrame()

for t in flat_list: 
    miovpull = MiovPuller(t, key)
    test = miovpull.get_response()
    response = miovpull.process_response(test)
    final = final.append(pd.DataFrame(response), ignore_index=True)
    #print(pd.unique(response))
    #sleep(1)

final.drop_duplicates(inplace = True, ignore_index = True)
final.rename(columns={0: "time", 1: "intersection_id", 2: "alert"}, inplace = True)

import copy
temp = copy.copy(final)

temp['time'] = pd.to_datetime(temp['time'])

temp = temp.sort_values(by=['intersection_id', 'alert', 'time'])
temp.reset_index(drop = True, inplace = True)

temp['shifted'] = temp.groupby(by = ['intersection_id', 'alert'])['time'].shift(1)
temp['time_lag'] = temp['time'] - temp['shifted']
temp['start_time'] = temp['time']

for index, row in temp.iterrows():
    if (row['time_lag'] == pd.Timedelta('0 days 00:05:00')):
        temp.at[index, 'start_time'] = temp.at[index-1, 'start_time']

#find start and end time of group 
temp.groupby(by = ['start_time', 'intersection_id', 'alert']).agg({'time': ['min', 'max']})
  