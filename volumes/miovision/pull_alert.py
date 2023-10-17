import datetime
import json
from requests import Session
import dateutil.parser
import logging
import configparser
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

step_size = 1 #in minutes
dt = '2023-10-15'
start = datetime.datetime.strptime(dt, "%Y-%m-%d")
flat_list = [start + datetime.timedelta(minutes=x) for x in range(0, 24 * 60, step_size)]

dfs = []

for t in flat_list: 
    miovpull = MiovPuller(t, key)
    test = miovpull.get_response()
    response = miovpull.process_response(test)
    df = pd.DataFrame(response).drop_duplicates()
    dfs.append(df)
    
final = pd.concat(dfs, ignore_index=True)
final.rename(columns={0: "time", 1: "intersection_id", 2: "alert"}, inplace = True)
final['time'] = pd.to_datetime(final['time'])

final = final.sort_values(by=['intersection_id', 'alert', 'time'])
final.reset_index(drop = True, inplace = True)
final.drop_duplicates(inplace = True, ignore_index = True)

#group by alert and time
final['shifted'] = final.groupby(by = ['intersection_id', 'alert'])['time'].shift(1)
final['time_lag'] = final['time'] - final['shifted']
final['start_time'] = final['time']

#iterate through and check if consecutive
for index, row in final.iterrows():
    if (row['time_lag'] == pd.Timedelta(f"{step_size} minutes")): #lag = step size
        final.at[index, 'start_time'] = final.at[index-1, 'start_time'] #assign previous start time

#find start and end time of group 
summary = final.groupby(by = ['start_time', 'intersection_id', 'alert']).agg({'time': ['max']})
summary = summary.reset_index()

print(summary)