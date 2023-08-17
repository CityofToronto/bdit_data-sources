import requests, json
from configparser import ConfigParser
from psycopg2 import connect

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
            'begin': '2023-07-01T00:00:00',
            'end':   '2023-07-02T00:00:00',
            'complete': 'false'
            #'step'
        }
    )
    return response.json()

for site in getSites():
    # check that we know of the site
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM ecocounter.sites WHERE site_id = %s",
            (site['id'],)
        )
        if cursor.rowcount < 1:
            continue
        for channel in site['channels']:
            channel_id = channel['id']
            print(channel_id)
            print(getChannelData(channel_id))
