import requests, json
from configparser import ConfigParser
from psycopg2 import connect
from datetime import datetime

config = ConfigParser()
config.read(r'/home/nwessel/db-creds.config')
connection = connect(**config['dbauth'])
connection.autocommit = True

endpoint = 'https://apieco.eco-counter-tools.com'

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

token = getToken()

def getSites():
    response = requests.get(
        f'{endpoint}/api/site',
        headers={'Authorization': f'Bearer {token}'}
    )
    return response.json()

def getChannelData(channel_id):
    response = requests.get(
        f'{endpoint}/api/data/site/{channel_id}',
        headers={'Authorization': f'Bearer {token}'},
        params={
            'begin': '2010-01-01T00:00:00',
            'end':   '2023-07-02T00:00:00',
            'complete': 'false'
        }
    )
    return response.json()

def siteIsKnownToUs(site_id):
    # do we have a record of this site in the database?
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

def insertFlowCount(flow_id, datetime_bin, volume):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ecocounter.counts (flow_id, datetime_bin, volume)
            VALUES ( %s, %s, %s );
            """,
            (flow_id, datetime_bin, volume)
        )

for site in getSites():
    if not siteIsKnownToUs(site['id']):
        continue

    for channel in site['channels']:
        if not flowIsKnownToUs(channel['id']):
            continue

        # we have this site and channel in the database
        channel_id = channel['id']
        print(channel_id)
        # empty the count table for this flow
        truncateFlow(channel_id)
        # and fill it back up!
        counts = getChannelData(channel_id)
        for count in counts:
            volume = count['counts']
            insertFlowCount(channel_id, count['date'], volume)
            print(count['date'])
        #raise SystemExit