import calendar
import configparser
import logging
import os
import re
import shutil
import subprocess
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from time import sleep

import click
import pysftp

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
def _send_data_to_database(datafile = None, dbsetting=None, dbconfig=None):
    return send_data_to_database(datafile, dbsetting, dbconfig)

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

@cli.command('get')
@click.argument('host')
@click.argument('user')
@click.argument('password')
@click.argument('date')
def _get_data(host = None, user=None, password=None, date=None):
    return get_data(host, user, password, date)

def get_data(host = None, user=None, password=None, date=None):
    '''Transfer data file for date'''
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys.load(os.path.expanduser('~/.ssh/known_hosts'))
    with pysftp.Connection(host, username=user, password=password, port=2222, cnopts=cnopts):
        #TODO


def pull_cis_data(ctx, startdate, enddate, config):

    configuration = configparser.ConfigParser()
    configuration.read(config)
    dbsettings = configuration['DBSETTINGS']
    sftp_cfg = configuration['SFTP']
    email = configuration['EMAIL']
    FORMAT = '%(asctime)s %(name)-2s %(levelname)-2s %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)

    try:
              
        filename = get_data(sftp_cfg['host'], sftp_cfg['user'], sftp_cfg['password'], date)

        send_data_to_database(datafile=filename+'.csv.gz', dbsetting=dbsettings)
    except TTCSFTPException as ttc_exc:
        LOGGER.critical('Fatal error in pulling data')
        LOGGER.critical(ttc_exc)
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