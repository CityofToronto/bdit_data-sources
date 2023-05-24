import sys
import json
from requests import Session
from requests import exceptions
import datetime
import pytz
import dateutil.parser
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import connect, Error
import logging
import configparser
import click
import traceback
from time import sleep
from collections import namedtuple

def logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter=logging.Formatter('%(asctime)s     	%(levelname)s    %(message)s', datefmt='%d %b %Y %H:%M:%S')
    stream_handler=logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger

logger = logger()
logger.debug('Start')

time_delta = datetime.timedelta(days=1)
default_start = str(datetime.date.today()-time_delta)
default_end = str(datetime.date.today())

session = Session()
session.proxies = {}
url = 'https://api.miovision.com/alerts/'

CONTEXT_SETTINGS = dict(
    default_map={'run_api': {'flag': 0}}
)

#start_date {{ds}} --end_date {{tomorrow_ds}}
start_date = '2022-05-23'

def run_api(start_date, end_date, path, intersection, pull, dupes):

    CONFIG = configparser.ConfigParser()
    #CONFIG.read(path)
    CONFIG.read('/etc/airflow/data_scripts/volumes/miovision/api/config.cfg')
    api_key=CONFIG['API']
    key=api_key['key']
    dbset = CONFIG['DBSETTINGS']
    conn = connect(**dbset)
    conn.autocommit = True
    logger.debug('Connected to DB')

    start_time = dateutil.parser.parse(str(start_date))
    end_time = dateutil.parser.parse(str(end_date))
    logger.info('Pulling from %s to %s' %(start_time, end_time))

    try:
        pull_data(conn, start_time, end_time, intersection, path, pull, key, dupes)
    except Exception as e:
        logger.critical(traceback.format_exc())
        sys.exit(1)
