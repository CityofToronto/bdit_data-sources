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

from notify_email import send_mail


class TTCSFTPException(Exception):
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
    dt = datetime.today() - timedelta(days=4)
    return dt.date().strftime('%Y%m%d')

def default_end_date():
    dt = datetime.today() - timedelta(days=3)
    return dt.date().strftime('%Y%m%d')

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
        pull_cis_data(ctx, startdate, enddate, config)

@cli.command('upload')
@click.argument('dbconfig', type=click.Path(exists=True))
@click.argument('datafile', type=click.Path(exists=True))
def send_data_to_database(datafile = None, dbsetting=None, dbconfig=None):
    '''Unzip the file and pipe the data to a database COPY statement'''
    if dbconfig:
        configuration = configparser.ConfigParser()
        configuration.read(dbconfig)
        dbsetting = configuration['DBSETTINGS']

    LOGGER.info('Sending data to database')
    try:
        #First subprocess needs to use Popen because piping stdout
        unzip = subprocess.Popen(['gunzip','-c',datafile], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #Second uses check_call and 'ON_ERROR_STOP=1' to make sure errors are captured and that the third 
        #process doesn't run befor psql is finished.
        LOGGER.info(subprocess.check_output(['psql','-h', dbsetting['host'],'-U',dbsetting['user'],'-d','bigdata','-v','ON_ERROR_STOP=1',
                                        '-c',r'\COPY here.ta_staging FROM STDIN WITH (FORMAT csv, HEADER TRUE); INSERT INTO here.ta SELECT * FROM here.ta_staging; TRUNCATE here.ta_staging;'],
                                        stdin=unzip.stdout))
        subprocess.check_call(['rm', datafile])
    except subprocess.CalledProcessError as err:
        LOGGER.critical('Error sending data to database')
        raise TTCSFTPException(err.stderr)

def pull_cis_data(ctx, startdate, enddate, config):

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
        ctx.invoke(download_data, download_url=download_url, filename=filename)

        ctx.invoke(send_data_to_database, datafile=filename+'.csv.gz', dbsetting=dbsettings)
    except TTCSFTPException as here_exc:
        LOGGER.critical('Fatal error in pulling data')
        LOGGER.critical(here_exc)
        #send_mail(email['to'], email['from'], email['subject'], str(here_exc))
    except Exception:
        LOGGER.critical(traceback.format_exc())
        # Only send email if critical error
        #send_mail(email['to'], email['from'], email['subject'], traceback.format_exc())

def main():
    #https://github.com/pallets/click/issues/456#issuecomment-159543498
    cli(obj={})  

if __name__ == '__main__':
    main()