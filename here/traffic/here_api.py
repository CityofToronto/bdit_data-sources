import calendar
import configparser
import logging
import logging.handlers
import re
import shutil
import subprocess
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from time import sleep

import click
import requests
from requests_oauthlib import OAuth1


class HereAPIException(Exception):
    '''Base Exception for all errors thrown by this module'''

class BufferingSMTPHandler(logging.handlers.BufferingHandler):
    '''From https://gist.github.com/anonymous/1379446
    
    Copyright (C) 2001-2002 Vinay Sajip. All Rights Reserved.
    '''

    def __init__(self, fromaddr, toaddrs, subject, capacity):
        logging.handlers.BufferingHandler.__init__(self, capacity)
        self.fromaddr = fromaddr
        self.toaddrs = toaddrs
        self.subject = subject
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s %(message)s"))

    def flush(self):
        if len(self.buffer) > 0:
            try:
                from notify_email import send_mail
                msg = ''
                for record in self.buffer:
                    s = self.format(record)
                    msg = msg + s + "\r\n"
                send_mail(self.toaddrs, self.fromaddr, self.subject, msg)
            except:
                self.handleError(None)  # no particular record
            self.buffer = []

LOGGER = logging.getLogger(__name__)

def _get_date_yyyymmdd(yyyymmdd):
    datetime_format = '%Y%m%d'
    try:
        date = datetime.strptime(str(yyyymmdd), datetime_format)
    except ValueError:
        raise ValueError('{yyyymmdd} is not a valid year-month value of format YYYYMMDD'
                         .format(yyyymmdd=yyyymmdd))
    return date

def default_start_date():
    dt = datetime.today() - timedelta(days=8)
    return dt.date().strftime('%Y%m%d')

def default_end_date():
    dt = datetime.today() - timedelta(days=2)
    return dt.date().strftime('%Y%m%d')

def get_access_token(key_id, key_secret, token_url):
    '''Uses Oauth1 to get an access token using the key_id and client_secret'''
    oauth1 = OAuth1(key_id, client_secret=key_secret)
    headers = {'content-type': 'application/json'}
    payload = {'grantType':'client_credentials', 'expiresIn': 3600}
    LOGGER.info('Getting Access Token')
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

    LOGGER.info('Querying data from %s to %s', str(start_date.date()), str(end_date.date()))
    query_header = {'Authorization':'Bearer '+ access_token, 'Content-Type': 'application/json'}

    query_response = requests.post(query_url, headers=query_header, json=query)
    try:
        query_response.raise_for_status()
    except requests.exceptions.HTTPError:
        LOGGER.error('Error in requesting query')
        raise HereAPIException(query_response.json()['message'])
    return str(query_response.json()['requestId'])

def get_download_url(request_id, status_base_url, access_token, user_id):
    '''Pings to get status of request and then returns the download URL when it has successfully completed'''

    status='Pending'
    status_url = status_base_url + str(user_id) + '/requests/' + str(request_id)
    status_header = {'Authorization': 'Bearer ' +  access_token}

    while status != "Completed Successfully":
        sleep(60)
        LOGGER.info('Polling status of query request: %s', request_id)
        query_status = requests.get(status_url, headers = status_header)
        status = str(query_status.json()['status'])
    LOGGER.info('Requested query completed')
    return query_status.json()['outputUrl']

def download_data(download_url, filename):
    '''Download data from specified url to specified filename'''
    LOGGER.info('Downloading data')
    download = requests.get(download_url, stream=True)

    with open(filename+'.csv.gz', 'wb') as f:
        shutil.copyfileobj(download.raw, f)

def send_data_to_database(dbsetting, filename):
    '''Unzip the file and pipe the data to a database COPY statement'''
    cmd = 'gunzip -c ' + filename 
    cmd += ' | psql -h '+ dbsetting['host'] +' -d bigdata -v "ON_ERROR_STOP=1"'
    cmd += r'-c "\COPY here.ta_staging FROM STDIN WITH (FORMAT csv, HEADER TRUE); INSERT INTO here.ta SELECT * FROM here.ta_staging; TRUNCATE here.ta_staging;"'
    LOGGER.info('Sending data to database')
    try:
        subprocess.run(cmd, shell=True, stderr=subprocess.PIPE)
        subprocess.run(['rm', filename], stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as err:
        LOGGER.critical('Error sending data to database')
        LOGGER.critical(err.stderr)


@click.command()
@click.option('-s','--startdate', default=default_start_date())
@click.option('-e','--enddate', default=default_end_date())
@click.option('-d','--config', default='db.cfg')
def main(startdate, enddate, config):
    '''Pull data from the HERE Traffic Analytics API from --startdate to --enddate
    
    The default is to process the previous week of data, with a 1+ day delay (running Monday-Sunday from the following Tuesday).
    
    '''
    configuration = configparser.ConfigParser()
    configuration.read(config)
    dbsettings = configuration['DBSETTINGS']
    apis = configuration['API']
    email = configuration['EMAIL']

    LOGGER.setLevel(logging.INFO)
    LOGGER.addHandler(BufferingSMTPHandler(email['from'], email['to'], email['subject'], 20))
    LOGGER.addHandler(logging.FileHandler('here_api.log').setFormatter('%(asctime)s %(levelname)-5s %(message)s'))
    try:
        access_token = get_access_token(apis['key_id'], apis['client_secret'], apis['token_url'])

        request_id = query_dates(access_token, _get_date_yyyymmdd(startdate), _get_date_yyyymmdd(enddate), apis['query_url'], apis['user_id'], apis['user_email'])

        download_url = get_download_url(request_id, apis['status_base_url'], access_token, apis['user_id'])
        filename = 'here_data_'+str(startdate)+'_'+str(enddate)
        download_data(download_url, filename)

        send_data_to_database(dbsettings, filename+'.csv.gz')
    except HereAPIException as here_exc:
        LOGGER.critical('Fatal error in pulling data')
        LOGGER.critical(here_exc)
        logging.shutdown()
    except Exception:
        LOGGER.critical(traceback.format_exc())
        # Only send email if critical error
        logging.shutdown()
