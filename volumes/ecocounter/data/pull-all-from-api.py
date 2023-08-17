import requests, json
from configparser import ConfigParser
#from psycopg2 import connect

config = ConfigParser()
#config.read(r'/home/nwessel/db-creds.config')
#connection = connect(**config['dbauth'])
#connection.autocommit = True

endpoint = 'https://apieco.eco-counter-tools.com'

def getToken():
    config.read(r'volumes/ecocounter/.ecocounter-credentials.config')
    response = requests.post(
        f'{endpoint}/token',
        headers={
            'Authorization':'Basic eVJlR1J0NDJWTkVxV1p1aTNEa0tRSm1CNXFRYTpYeFFmdGJoNDRoVXF5ck5rbTRwU21WVVJ4Nmdh'
        },
        data={
            'grant_type': 'password',
            'username': config['DEFAULT']['username'],
            'password': config['DEFAULT']['password']
        }
    )
    return response.json()['access_token']

print(getToken())

#headers = {'Authorization': f'Bearer {token}'}

#sites_response = requests.get(f'{endpoint}/site',headers=headers)


raise SystemExit

for site in json.loads(sites_response.text):
    # check that we know of the site
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT 1 FROM ecocounter.sites WHERE site_id = %s",
            (site['id'],)
        )
        if cursor.rowcount < 1:
            continue

        site_id = site['id']

        print(json.dumps(site,indent=4))

        break
        data_response = requests.get(
            f'{endpoint}/data/site/{site_id}/?complete=false&begin=2016-01-31T13:30:00&end=2023-05-31T13:30:00'
            'begin=2015-01-01T00:00:00',
            headers=headers
        )
        print(data_response.text)
