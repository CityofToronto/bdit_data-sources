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
from time_parsing import validate_multiple_yyyymmdd_range, get_yyyymmdd

class TTCSFTPException(Exception):
    '''Base Exception for all errors thrown by this module'''

LOGGER = logging.getLogger(__name__)

BASE_FILENAME = 'KingSteetPilot'
REMOTE_FILEPATH = 'cityoftor_kingstpilot'
LOCAL_FILEPATH = '/data/ttc/cis'

def default_date():
    dt = datetime.today() - timedelta(days=1)
    return dt.date().strftime('%Y%m%d')

@click.group(invoke_without_command=True)
@click.option('-s','--startdate', default=default_date(), help='YYYYMMDD')
@click.option('-e','--enddate', default=default_date(), help='YYYYMMDD')
@click.option('-d','--config', type=click.Path(exists=True), help='.cfg file containing db, email, and sftp settings', default='config.cfg')
@click.option('--filename', type=click.Path(exists=True), help='filename to pull from sftp instead of using dates.')
@click.pass_context
def cli(ctx, startdate=None, enddate=None, config='config.cfg', filename=None):
    '''Pull CIS data from the TTC's sftp server from --startdate to --enddate

    The default is to grab yesterday's data. If using --startdate to --enddate, will loop through those dates 
    inclusively to pull each day of data. If a dump of data spanning multiple days is available, use 
    --filename instead
    
    '''

    if startdate != enddate and filename:
        raise click.BadOptionUsage('Cannot use --startdate/--enddate and --filename')

    ctx.obj['config'] = config

    if ctx.invoked_subcommand is None:
        pull_cis_data(config, startdate, enddate, filename)
    elif startdate != enddate:
        raise click.BadOptionUsage('Cannot use different --startdate/--enddate with subcommands')
    else:
        ctx.obj['date'] = enddate
        ctx.obj['filename'] = filename


@cli.command('get', short_help="Copy datafile from TTC's sftp server")
@click.option('--date', help='Specify the date to pull in YYYYMM.' + \
                             'Cannot be used in conjunction with startdate/enddate')
@click.pass_context
def _get_data(ctx, date=None):
    '''Copy a CIS datafile from the TTC's sftp server to the default destination. 
    Defaults to yesterday's date or you can specify a --date DATE in YYYYMMDD format. 
    Assumes there is a default `config.cfg` in the same folder
    or else you must specify a config file with --config conf.cfg before the get command
    
    Ex: pull_data_cis --config CONFIG get --date YYYYMMDD
    '''
    configuration = configparser.ConfigParser()
    configuration.read(ctx.obj['config'])
    sftp_settings = configuration['SFTP']
    host = sftp_settings['host']
    user = sftp_settings['user']
    password = sftp_settings['password']
    if date is None:
        date = ctx.obj['date']
    return get_data(host, user, password, date, filename=ctx.obj['filename'])

def get_data(host: str = None, user:str =None, password: str = None,
             date: str = None, filename: str = None):
    '''Transfer data file for date'''
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys.load(os.path.expanduser('~/.ssh/known_hosts'))
    with pysftp.Connection(host, username=user, password=password, port=2222, cnopts=cnopts) as sftp:
        if filename is None:
            filename = BASE_FILENAME + '_' + date + '_' + date + '.csv.gz'
        sftp.get(REMOTE_FILEPATH+'/'+filename, LOCAL_FILEPATH+'/'+filename)
    return LOCAL_FILEPATH +'/'+filename

@cli.command('upload', short_help="Upload datafile to database")
@click.argument('datafile', type=click.Path(exists=True))
@click.pass_context
def _send_data_to_database(ctx: click.Context, datafile = None):
    '''Upload datafile [DATAFILE] to the database. Assumes there is a default `config.cfg` in the same folder
    or else you must specify a config file with --config conf.cfg before the get command
    
    Ex: pull_data_cis --config CONFIG upload FILENAME'''
    configuration = configparser.ConfigParser()
    configuration.read(ctx.obj['config'])
    dbsettings = configuration['DBSETTINGS']
    return send_data_to_database(datafile, dbsetting=dbsettings)

def send_data_to_database(datafile = None, dbsetting=None, dbconfig=None):
    '''Unzip the file and pipe the data to a database COPY statement'''
    if dbconfig:
        configuration = configparser.ConfigParser()
        configuration.read(dbconfig)
        dbsetting = configuration['DBSETTINGS']

    LOGGER.debug('Sending data from %s to database', datafile)
    try:
        #First subprocess needs to use Popen because piping stdout
        unzip = subprocess.Popen(['gunzip','-c',datafile], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #Second uses check_call and 'ON_ERROR_STOP=1' to make sure errors are captured and that the third 
        #process doesn't run befor psql is finished.
        output = subprocess.check_output(['psql','-h', dbsetting['host'],'-U',dbsetting['user'],'-d','bigdata','-v','ON_ERROR_STOP=1',
                                        '-c',r"\COPY ttc.cis(message_datetime, route, run, vehicle, latitude, longitude)  FROM STDIN WITH (FORMAT csv, HEADER TRUE, DELIMITER '|');"],
                                        stdin=unzip.stdout)
        LOGGER.debug(output)
        subprocess.check_call(['rm', datafile])
    except subprocess.CalledProcessError as err:
        LOGGER.critical('Error sending %s to database', datafile)
        raise TTCSFTPException(err.stderr)

def get_and_upload_data(dbsetting: dict = None, email: dict = None, sftp_cfg:dict = None,
                        date: str = None, filename: str = None):
    try:
        LOGGER.info('Pulling CIS data for %s', date)
        filename = get_data(sftp_cfg['host'], sftp_cfg['user'], sftp_cfg['password'], 
                            date=date, filename=filename)

        send_data_to_database(datafile=filename, dbsetting=dbsetting)
    except TTCSFTPException as ttc_exc:
        LOGGER.critical('Fatal error in pulling data')
        LOGGER.critical(ttc_exc)
        send_mail(email['to'], email['from'], email['subject'], str(ttc_exc))
    except Exception:
        LOGGER.critical(traceback.format_exc())
        # Only send email if critical error
        send_mail(email['to'], email['from'], email['subject'], traceback.format_exc())

def pull_cis_data(config: str, startdate: str, enddate: str, filename: str = None):
    configuration = configparser.ConfigParser()
    configuration.read(config)
    dbsettings = configuration['DBSETTINGS']
    sftp_cfg = configuration['SFTP']
    email = configuration['EMAIL']
    FORMAT = '%(asctime)s %(name)-2s %(levelname)-2s %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    LOGGER.info('Pulling CIS data from %s to %s', startdate, enddate)

    if filename:
        get_and_upload_data(dbsettings, email, sftp_cfg, filename=filename)
    else:
        dates = validate_multiple_yyyymmdd_range([[startdate, enddate]])
        for year in dates:
            for month in dates[year]:
                for dd in dates[year][month]:
                    get_and_upload_data(dbsettings, email, sftp_cfg, date=get_yyyymmdd(year, month, dd))

def main():
    #https://github.com/pallets/click/issues/456#issuecomment-159543498
    cli(obj={})  

if __name__ == '__main__':
    main()
