import configparser
import logging
import logging.handlers
import shutil
import subprocess
import traceback
from datetime import datetime, timedelta
from json import JSONDecodeError
import os 
import sys
import click
import requests

from here_api import get_download_url, get_access_token

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

def query_dates(access_token, start_date, end_date, query_url, user_id, user_email,
                request_type = 'PATH', vehicle_type = 'ALL', epoch_type = 5):
    query= {"queryFilter": {"requestType":request_type,
                            "vehicleType":vehicle_type,
                            "adminId":21055226,
                            "adminLevel":3,
                            "isoCountryCode":"CAN",
                            "startDate":datetime.strptime(start_date, '%Y%m%d').date().strftime("%Y-%m-%d"),
                            "endDate":datetime.strptime(end_date, '%Y%m%d').date().strftime("%Y-%m-%d"),
                            "timeIntervals":[],
                            "locationFilter":{"tmcs":[]},
                            "daysOfWeek":{"U":True,"M":True,"T":True,"W":True,"R":True,"F":True,"S":True},
                            },
            "outputFormat":{"mean":True,
                            "tmcBased":False,
                            "epochType":epoch_type,
                            "percentiles":[50,85],
                            "minMax":True,
                            "stdDev":True,
                            "confidence":True,
                            "freeFlow":False,
                            "length_":True,
                            "gapFilling":False,
                            "speedLimit":False,
                            "sampleCount":True},
            "estimatedSize":0,
            "userId":user_id,
            'userEmail':user_email}

    LOGGER.info('Querying data from %s to %s', str(start_date), str(end_date))
    query_header = {'Authorization':'Bearer '+ access_token, 'Content-Type': 'application/json'}

    query_response = requests.post(query_url, headers=query_header, json=query)
    try:
        query_response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        LOGGER.error('Error in requesting query')
        LOGGER.error(err)
        try:
            err_msg = query_response.json()['message']
        except JSONDecodeError:
            err_msg = query_response.text
        finally:
            raise HereAPIException(err_msg)
    return str(query_response.json()['requestId'])

@click.group(invoke_without_command=True)
@click.option('-s','--startdate', default=default_start_date())
@click.option('-e','--enddate', default=default_end_date())
@click.option('-d','--config', type=click.Path(exists=True))
@click.pass_context
def cli(ctx, startdate=default_start_date(), enddate=default_end_date(), config='db.cfg'):
    '''Pull data from the HERE Traffic Analytics API from --startdate to --enddate (inclusive)

    The default is to process the previous week of data, with a 1+ day delay (running Monday-Sunday from the following Tuesday).
    
    '''
    FORMAT = '%(asctime)s %(name)-2s %(levelname)-2s %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    ctx.obj['config'] = config
    if ctx.invoked_subcommand is None:
        pull_here_data(ctx, startdate, enddate)

@cli.command('download')
@click.argument('download_url')
@click.argument('filename')
@click.pass_context
def download_data(ctx = None, download_url = None, filename = None):
    '''Download data from specified url to specified filename'''
    LOGGER.info('Downloading data')
    download = requests.get(download_url, stream=True)

    with open(filename+'.csv.gz', 'wb') as f:
        shutil.copyfileobj(download.raw, f)

@cli.command('upload')
@click.argument('datafile', type=click.Path(exists=True))
@click.pass_context
def send_data_to_database(ctx=None, datafile = None, dbsetting=None):
    '''Unzip the file and pipe the data to a database COPY statement'''
    if not dbsetting and not os.getenv('here_bot'):
        configuration = configparser.ConfigParser()
        configuration.read(ctx.obj['config'])
        dbsetting = configuration['DBSETTINGS']

    LOGGER.info('Sending data to database')
    try:
        #First subprocess needs to use Popen because piping stdout
        unzip = subprocess.Popen(['gunzip','-c',datafile], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #Second uses check_call and 'ON_ERROR_STOP=1' to make sure errors are captured and that the third 
        #process doesn't run befor psql is finished.
        copy = r'''"\COPY here.ta_path_view FROM STDIN WITH (FORMAT csv, HEADER 
                    TRUE);"'''
        if os.getenv('here_bot'):
            #there's a here_bot environment variable to connect to postgresql.
            #use the environment variable, which requires running subprocess
            #with env=os.environ.copy(), shell=True
            #Note that with shell=True, the command must be one long string.
            cmd = '''psql $here_bot -v "ON_ERROR_STOP=1" -c {copy}'''.format(copy=copy)
            LOGGER.info(subprocess.check_output(cmd,
            stdin=unzip.stdout, env=os.environ.copy(), shell=True))
        else:
            LOGGER.warning('No here_bot environment variable detected, assuming .pgpass value exists')
            LOGGER.info(subprocess.check_output(['psql','-h', dbsetting['host'],'-U',dbsetting['user'],'-d','bigdata','-v','"ON_ERROR_STOP=1"',
                                        '-c',copy],
                                        stdin=unzip.stdout))
        subprocess.check_call(['rm', datafile])
    except subprocess.CalledProcessError as err:
        LOGGER.critical('Error sending data to database')
        raise HereAPIException(err.stderr)

def pull_here_data(ctx, startdate, enddate):

    configuration = configparser.ConfigParser()
    configuration.read(ctx.obj['config'])
    dbsettings = configuration['DBSETTINGS']
    apis = configuration['API']
    email = configuration['EMAIL']

    try:
        access_token = get_access_token(apis['key_id'], apis['client_secret'], apis['token_url'])

        request_id = query_dates(access_token, _get_date_yyyymmdd(startdate), _get_date_yyyymmdd(enddate), apis['query_url'], apis['user_id'], apis['user_email'])

        download_url = get_download_url(request_id, apis['status_base_url'], access_token, apis['user_id'])

        filename = 'here_data_'+str(startdate)+'_'+str(enddate)
        ctx.invoke(download_data, download_url=download_url, filename=filename)

        ctx.invoke(send_data_to_database, datafile=filename+'.csv.gz', dbsetting=dbsettings)
    except HereAPIException as here_exc:
        LOGGER.critical('Fatal error in pulling data')
        LOGGER.critical(here_exc)
        sys.exit(1)
    except Exception:
        LOGGER.critical(traceback.format_exc())
        sys.exit(2)

def main():
    #https://github.com/pallets/click/issues/456#issuecomment-159543498
    cli(obj={})  

if __name__ == '__main__':
    main()