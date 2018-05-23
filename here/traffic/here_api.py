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

from notify_email import send_mail

class HereAPIException(Exception):
    '''Base Exception for all errors thrown by this module'''

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
    dt = datetime.today() - timedelta(days=9)
    return dt.date().strftime('%Y%m%d')

def default_end_date():
    dt = datetime.today() - timedelta(days=3)
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

def query_dates(access_token, start_date, end_date, query_url, user_id, user_email,
                request_type = 'PROBE_PATH', vehicle_type = 'ALL', epoch_type = 5):
    query= {"queryFilter": {"requestType":request_type,
                            "vehicleType":vehicle_type,
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
                            "epochType":epoch_type,
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
        try:
            status = str(query_status.json()['status'])
        except KeyError as _:
            LOGGER.error('Missing "status" in response')
            raise HereAPIException(query_status.text)
    LOGGER.info('Requested query completed')
    return query_status.json()['outputUrl']

@click.group(invoke_without_command=True)
@click.option('-s','--startdate', default=default_start_date())
@click.option('-e','--enddate', default=default_end_date())
@click.option('-d','--config', type=click.Path(exists=True))
@click.pass_context
def cli(ctx, startdate=default_start_date(), enddate=default_end_date(), config='db.cfg'):
    '''Pull data from the HERE Traffic Analytics API from --startdate to --enddate

    The default is to process the previous week of data, with a 1+ day delay (running Monday-Sunday from the following Tuesday).
    
    '''
    if ctx.invoked_subcommand is None:
        pull_here_data(startdate, enddate, config)

@cli.command('download')
@click.argument('download_url')
@click.argument('filename')
def download_data(download_url, filename):
    '''Download data from specified url to specified filename'''
    LOGGER.info('Downloading data')
    download = requests.get(download_url, stream=True)

    with open(filename+'.csv.gz', 'wb') as f:
        shutil.copyfileobj(download.raw, f)

@cli.command('upload')
@click.argument('dbconfig', type=click.Path(exists=True))
@click.argument('datafile', type=click.Path(exists=True))
def send_data_to_database(datafile, dbsetting=None, dbconfig=None):
    '''Unzip the file and pipe the data to a database COPY statement'''
    if dbconfig:
        configuration = configparser.ConfigParser()
        configuration.read(dbconfig)
        dbsetting = configuration['DBSETTINGS']

    LOGGER.info('Sending data to database')
    try:
        unzip = subprocess.Popen(['gunzip','-c',datafile], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        upload_data = subprocess.Popen(['psql','-h', dbsetting['host'],'-U',dbsetting['user'],'-d','bigdata','-v','"ON_ERROR_STOP=1"',
                                        '-c',r'"\COPY here.ta_staging FROM STDIN WITH (FORMAT csv, HEADER TRUE); INSERT INTO here.ta SELECT * FROM here.ta_staging; TRUNCATE here.ta_staging;"'],
                                        stdin=unzip.stdout, stderr=subprocess.PIPE)
        subprocess.run(['rm', datafile], stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as err:
        LOGGER.critical('Error sending data to database')
        raise HereAPIException(err.stderr)

def pull_here_data(startdate, enddate, config):

    configuration = configparser.ConfigParser()
    configuration.read(config)
    dbsettings = configuration['DBSETTINGS']
    apis = configuration['API']
    email = configuration['EMAIL']
    FORMAT = '%(asctime)s %(name)-2s %(levelname)-2s %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)

    try:
        access_token = get_access_token(apis['key_id'], apis['client_secret'], apis['token_url'])

        request_id = query_dates(access_token, _get_date_yyyymmdd(startdate), _get_date_yyyymmdd(enddate), apis['query_url'], apis['user_id'], apis['user_email'])

        download_url = get_download_url(request_id, apis['status_base_url'], access_token, apis['user_id'])
        filename = 'here_data_'+str(startdate)+'_'+str(enddate)
        download_data(download_url = download_url, filename = filename)

        send_data_to_database(filename+'.csv.gz', dbsetting=dbsettings)
    except HereAPIException as here_exc:
        LOGGER.critical('Fatal error in pulling data')
        LOGGER.critical(here_exc)
        send_mail(email['to'], email['from'], email['subject'], str(here_exc))
    except Exception:
        LOGGER.critical(traceback.format_exc())
        # Only send email if critical error
        send_mail(email['to'], email['from'], email['subject'], traceback.format_exc())

def main():
    #https://github.com/pallets/click/issues/456#issuecomment-159543498
    cli(obj={})  

if __name__ == '__main__':
    main()