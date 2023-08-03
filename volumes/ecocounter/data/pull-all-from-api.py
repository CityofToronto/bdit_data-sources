import requests, json
from configparser import ConfigParser
from psycopg2 import connect

config = ConfigParser()
config.read(r'/home/nwessel/db-creds.config')
connection = connect(**config['dbauth'])
connection.autocommit = True

endpoint = 'https://apieco.eco-counter-tools.com/api/1.0'
token = input('Token? ')
headers = {'Authorization': f'Bearer {token}'}

sites_response = requests.get(f'{endpoint}/site',headers=headers)

for site in json.loads(sites_response.text):
    if site['latitude'] == 0:
        continue
    print(json.dumps(site,indent=4))

    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT * FROM ecocounter.sites WHERE site_id = %s",
            (site['id'],)
        )
        res = cursor.fetchone()
        print(res)
    break