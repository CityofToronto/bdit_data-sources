import requests, json

endpoint = 'https://apieco.eco-counter-tools.com/api/1.0'
token = input('Token? ')
headers = {'Authorization': f'Bearer {token}'}

response = requests.get(f'{endpoint}/site',headers=headers)

for site in json.loads(response.text):
    if site['latitude'] == 0:
        continue
    print(json.dumps(site,indent=4))
    break