import requests
from configparser import ConfigParser
from psycopg2 import connect
from datetime import datetime, timedelta
from tqdm import tqdm # this is a progress bar created by simply wrapping an iterable

config = ConfigParser()
config.read(r'/home/nwessel/db-creds.config')
connection = connect(**config['dbauth'])
connection.autocommit = True

endpoint = 'https://apieco.eco-counter-tools.com'

# get an authentication token for accessing the API
def getToken():
    config.read(r'volumes/ecocounter/.api-credentials.config')
    response = requests.post(
        f'{endpoint}/token',
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
def getSites():
    response = requests.get(
        f'{endpoint}/api/site',
        headers={'Authorization': f'Bearer {token}'}
    )
    return response.json()

# gt all of a channel/flow's data from the API
def getChannelData(channel_id,firstData='2015-01-01T00:00:00'):
    requestChunkSize = timedelta(days=100)
    requestStartDate = datetime.strptime(firstData,"%Y-%m-%dT%H:%M:%S%z")
    data = []
    while requestStartDate.timestamp() < datetime.now().timestamp():
        response = requests.get(
            f'{endpoint}/api/data/site/{channel_id}',
            headers={'Authorization': f'Bearer {token}'},
            params={
                'begin': requestStartDate.isoformat(timespec='seconds'), 
                'end':  (requestStartDate + requestChunkSize).isoformat(timespec='seconds'),
                'complete': 'false',
                'step': '15m'
            }
        )
        data += response.json()
        requestStartDate += requestChunkSize
    return data

# do we have a record of this site in the database?
def siteIsKnownToUs(site_id):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM ecocounter.sites WHERE site_id = %s",
            (site_id,)
        )
        return cursor.rowcount > 0

# do we have a record of this flow in the database?
def flowIsKnownToUs(flow_id):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM ecocounter.flows WHERE flow_id = %s",
            (flow_id,)
        )
        return cursor.rowcount > 0

# DANGER delete all count data for a given flow
def truncateFlow(flow_id):
    with connection.cursor() as cursor:
        cursor.execute(
            "DELETE FROM ecocounter.counts WHERE flow_id = %s",
            (flow_id,)
        )

# insert a single count record
def insertFlowCount(flow_id, datetime_bin, volume):
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO ecocounter.counts (flow_id, datetime_bin, volume) VALUES (%s, %s, %s)",
            (flow_id, datetime_bin, volume)
        )

token = getToken()

for site in getSites():
#    if not site['id'] == 1234: # handy switch for doing a single site
#        continue

    # only update data for sites / channels in the database
    # but announce unknowns for manual validation if necessary
    if not siteIsKnownToUs(site['id']):
        print('unknown site', site['id'], site['name'])
        continue

    for channel in site['channels']:
        if not flowIsKnownToUs(channel['id']):
            print('unknown flow', channel['id'])
            continue

        # we do have this site and channel in the database; let's update its counts
        channel_id = channel['id']
        print(f'starting on flow {channel_id}')

        # empty the count table for this flow
        truncateFlow(channel_id)
 
        # and fill it back up!
        print(f'fetching data for flow {channel_id}')
        counts = getChannelData(channel_id, firstData=channel['firstData'])

        print(f'inserting data for flow {channel_id}')
        for count in tqdm(counts): # iterator with progress bar
            volume = count['counts']
            insertFlowCount(channel_id, count['date'], volume)
