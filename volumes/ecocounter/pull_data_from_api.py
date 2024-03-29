import requests
from configparser import ConfigParser
from psycopg2 import connect
from datetime import datetime, timedelta
#from tqdm import tqdm # this is a progress bar created by simply wrapping an iterable

default_start = datetime.fromisoformat('2015-01-01')
default_end = datetime.now().replace(hour = 0, minute = 0, second = 0, microsecond = 0)

url = 'https://apieco.eco-counter-tools.com'

# get an authentication token for accessing the API
def getToken(api_config_path):
    config = ConfigParser()
    config.read(api_config_path)
    response = requests.post(
        f'{url}/token',
        headers={
            'Authorization': 'Basic ' + config['DEFAULT']['secret_api_hash']
        },
        data={
            'grant_type': 'password',
            'username': config['DEFAULT']['username'],
            'password': config['DEFAULT']['password']
        }
    )
    return response.json()['access_token']

# get a list of all sites from the API
def getSites(token, sites=()):
    response = requests.get(
        f'{url}/api/site',
        headers={'Authorization': f'Bearer {token}'}
    )
    result = []
    if not sites == ():
        for site in response.json():
            if site['id'] in sites:
                result.append(site)
        return result
    else:
        return response.json()

# get all of a channel/flow's data from the API
def getChannelData(token, channel_id, startDate, endDate):
    requestChunkSize = timedelta(days=100)
    requestStartDate = startDate
    data = []
    while requestStartDate.timestamp() < endDate:
        response = requests.get(
            f'{url}/api/data/site/{channel_id}',
            headers={'Authorization': f'Bearer {token}'},
            params={
                'begin': requestStartDate.isoformat(timespec='seconds'),
                #may need to truncate this to endDate.
                'end':  (requestStartDate + requestChunkSize).isoformat(timespec='seconds'),
                'complete': 'false',
                'step': '15m'
            }
        )
        data += response.json()
        requestStartDate += requestChunkSize
    return data

# do we have a record of this site in the database?
def siteIsKnownToUs(site_id, conn):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM ecocounter.sites WHERE site_id = %s",
            (site_id,)
        )
        return cursor.rowcount > 0

# do we have a record of this flow in the database?
def flowIsKnownToUs(flow_id, conn):
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM ecocounter.flows WHERE flow_id = %s",
            (flow_id,)
        )
        return cursor.rowcount > 0

# DANGER delete all count data for a given flow
def truncateFlowSince(flow_id, conn, startDate, endDate):
    with conn.cursor() as cursor:
        cursor.execute(
            """DELETE FROM ecocounter.counts
            WHERE flow_id = %s
            AND datetime_bin >= %s
            AND datetime_bin < %s""",
            (flow_id, startDate, endDate)
        )

# insert a single count record
def insertFlowCount(flow_id, datetime_bin, volume, conn):
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO ecocounter.counts (flow_id, datetime_bin, volume) VALUES %s",
            (flow_id, datetime_bin, volume)
        )

def run_api(start_date=default_start, end_date=default_end, sites=()):

    config = ConfigParser()
    config.read(r'/data/home/nwessel/db-creds.config')
    conn = connect(**config['dbauth'])
    conn.autocommit = True

    API_CONFIG_PATH = 'volumes/ecocounter/.api-credentials.config'

    token = getToken(API_CONFIG_PATH)
       
    for site in getSites(token, sites=sites): #optionally specify site_ids here. 

        # only update data for sites / channels in the database
        # but announce unknowns for manual validation if necessary
        if not siteIsKnownToUs(site['id'], conn):
            print('unknown site', site['id'], site['name'])
            continue

        for channel in site['channels']:
            if not flowIsKnownToUs(channel['id'], conn):
                print('unknown flow', channel['id'])
                continue

            # we do have this site and channel in the database; let's update its counts
            channel_id = channel['id']
            print(f'starting on flow {channel_id}')

            # empty the count table for this flow
            truncateFlowSince(channel_id, conn, start_date, end_date)
    
            # and fill it back up!
            print(f'fetching data for flow {channel_id}')
            counts = getChannelData(token, channel_id, start_date, end_date)

            print(f'inserting data for flow {channel_id}')
            #for count in tqdm(counts): # iterator with progress bar
            for count in counts: # iterator with progress bar
                volume = count['counts']
                insertFlowCount(channel_id, count['date'], volume, conn)