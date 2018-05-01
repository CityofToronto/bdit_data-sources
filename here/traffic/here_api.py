import calendar
import configparser
import datetime
import re
import shutil
import subprocess
from collections import defaultdict
from datetime import datetime
from time import sleep

import requests

import click
from requests_oauthlib import OAuth1


def _get_date_yyyymmdd(yyyymmdd):
    datetime_format = '%Y%m%d'
    try:
        date = datetime.strptime(str(yyyymmdd), datetime_format)
    except ValueError:
        raise ValueError('{yyyymmdd} is not a valid year-month value of format YYYYMMDD'
                         .format(yyyymmdd=yyyymmdd))
    return date


def get_access_token(key_id, key_secret, token_url):
    '''Uses Oauth1 to get an access token using the key_id and client_secret'''
    oauth1 = OAuth1(key_id, client_secret=key_secret)
    headers = {'content-type': 'application/json'}
    payload = {'grantType':'client_credentials', 'expiresIn': 3600}

    r = requests.post(token_url, auth=oauth1, json=payload, headers=headers)

    access_token = r.json()['accessToken']
    return access_token

def query_dates(access_token, start_date, end_date, query_url, user_id, user_email):
    query= {"queryFilter": {"requestType":"PROBE_PATH",
                            "vehicleType":"ALL",
                            "adminId":21055226,
                            "adminLevel":3,
                            "isoCountryCode":"CAN",
                            "startDate":str(start_date.date()),
                            "endDate":str(end_date.date()),
                            "timeIntervals":[],
                            "locationFilter":{"tmcs":[]},
                            "daysOfWeek":{"U":True,"M":True,"T":True,"W":True,"R":True,"F":True,"S":True},
                            "mapVersion":"2017Q3"},
            "outputFormat":{"mean":True,
                            "tmcBased":False,
                            "epochType":5,
                            "percentiles":[5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95],
                            "minMax":True,
                            "stdDev":True,
                            "confidence":True,
                            "freeFlow":False,
                            "length_":True,
                            "gapFilling":False,
                            "speedLimit":False,
                            "sampleCount":False},
            "estimatedSize":0,
            "userId":user_id,
            'userEmail':user_email}

    query_header = {'Authorization':'Bearer '+ access_token, 'Content-Type': 'application/json'}

    query_response = requests.post(query_url, headers=query_header, json=query)
    return str(query_response.json()['requestId'])

def get_download_url(request_id, status_base_url, access_token, user_id):
    '''Pings to get status of request and then returns the download URL when it has successfully completed'''

    status='Pending'
    status_url = status_base_url + str(user_id) + '/requests/' + str(request_id)
    status_header = {'Authorization': 'Bearer ' +  access_token}

    while status != "Completed Successfully":
        sleep(60)
        query_status = requests.get(status_url, headers = status_header)
        status = str(query_status.json()['status'])

    return query_status.json()['outputUrl']

def download_data(download_url, filename):
    download = requests.get(download_url, stream=True)

    with open(filename+'.csv.gz', 'wb') as f:
        shutil.copyfileobj(download.raw, f)

def send_data_to_database(dbsetting, filename):
    subprocess.Popen('gunzip -c ' + filename +' | psql -h '+ dbsetting['host'] +r' -d bigdata -c "\COPY here.ta_staging FROM STDIN WITH (FORMAT csv, HEADER TRUE); INSERT INTO here.ta SELECT * FROM here.ta_staging; TRUNCATE here.ta_staging;"', shell=True)

@click.command()
@click.argument('startdate')
@click.argument('enddate')
def main(startdate, enddate):
    config = configparser.ConfigParser()
    config.read('db.cfg')
    dbsettings = config['DBSETTINGS']
    apis = config['API']

    access_token = get_access_token(apis['key_id'], apis['client_secret'], apis['token_url'])

    request_id = query_dates(access_token, _get_date_yyyymmdd(startdate), _get_date_yyyymmdd(enddate), apis['query_url'], apis['user_id'], apis['user_email'])

    download_url = get_download_url(request_id, apis['status_base_url'], access_token, apis['user_id'])
    filename = 'here_data_'+str(startdate)+'_'+str(enddate)
    download_data(download_url, filename)

    send_data_to_database(dbsettings, filename+'.csv.gz')


# if __name__ == '__main__':
#     main()
