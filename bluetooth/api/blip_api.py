"""blip_api.py
Script to pull Bluetooth data from the Blip api. 
main defaults to getting the previous day's data 
"""

import argparse
import configparser
import datetime
import logging
import os
import sys
#from syslog import LOG_DEBUG
import time
import traceback
import json
import psycopg2
from psycopg2 import connect, sql, InternalError, IntegrityError, DatabaseError
from datetime import date
from itertools import product
from typing import List

import urllib3
import zeep
from dateutil.relativedelta import relativedelta
from psycopg2.extras import Json, execute_values
from requests import RequestException, Session
from tenacity import retry, before_sleep_log, wait_exponential, retry_if_exception_type, RetryError
from zeep import Client
from zeep.transports import Transport

from time_parsing import validate_multiple_yyyymmdd_range

# Suppress HTTPS Warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LOGGER = logging.getLogger(__name__)


def parse_args(args, prog=None, usage=None):
    '''Parser for the command line arguments'''
    parser = argparse.ArgumentParser(description='Pull data from blip API and '
                                                 'send to database',
                                     prog=prog, usage=usage)
    parser.add_argument("-y", "--years", nargs=2, action='append',
                        help="Range of dates (YYYYMMDD) to operate over "
                        "from startdate to enddate, else defaults to "
                        "previous day. ",
                        metavar=('YYYYMMDD', 'YYYYMMDD'))
    parser.add_argument("-a", "--analysis", action='append', type= int,
                        help="Analysis ID to pull. Add more flags for multiple IDs. Otherwise pulls all routes.")
    parser.add_argument("-d", "--dbsetting",
                        default='config.cfg',
                        help="Filename with connection settings to the database "
                        "(default: opens %(default)s)")
    parser.add_argument("--direct",
                        action='store_true',
                        help="Use this flag to use the proxy if using from workstation. Do not use if running from terminal server")
    parser.add_argument("--live",
                        action='store_true',
                        help="Pull most recent clock hour of live data, for King Street Pilot")

    return parser.parse_args(args)


def get_data_for_config(blip, un: str, pw: str, config):
    """
    Pull Blip data for the given configuration
        :param blip: 
            The Blip wsdl Client
        :param un: 
            Username
        :param pw: 
            Password
        :param config:
            WSDL Data Export Configuration 
    """
    try:
        data = blip.service.exportPerUserData(un, pw, config)
    except RequestException as err:
        LOGGER.error(err)
        LOGGER.error(config)
        time.sleep(30)
        data = blip.service.exportPerUserData(un, pw, config)
    except Exception as exc:
        LOGGER.error(exc)
        LOGGER.error(config)
        time.sleep(15)
        data = blip.service.exportPerUserData(un, pw, config)
    return data

@retry(retry=retry_if_exception_type(psycopg2.InternalError),
       wait=wait_exponential(multiplier=15, max=900), before_sleep=before_sleep_log(LOGGER, logging.ERROR))
def _get_db(dbset):
    '''Create a psycopg2 DB object and retry for up to 15 minutes if the connection is unsuccessful'''
    return connect(**dbset)


def insert_data(data: list, dbset: dict, live: bool):
    '''
    Upload data to the database
    :param data:
        List of dictionaries, gets converted to list of tuples
    :param dbset:
        DB settings passed to psycopg2 to create a connection 
    '''
    num_rows = len(data)
    if num_rows > 0:
        LOGGER.info('Uploading %s rows to PostgreSQL', len(data))
        LOGGER.debug(data[0])
    else:
        LOGGER.warning('No data to upload')
        return 
    to_insert = []
    for dic in data:
        # convert each observation dictionary into a tuple row for inserting
        row = (dic["userId"], dic["analysisId"], dic["measuredTime"],
               dic["measuredTimeNoFilter"], dic["startPointNumber"],
               dic["startPointName"], dic["endPointNumber"], dic["endPointName"],
               dic["measuredTimeTimestamp"], dic["outlierLevel"], dic["cod"],
               dic["deviceClass"])
        to_insert.append(row)
    try:
        conn = _get_db(dbset)
    except RetryError as retry_err:
        LOGGER.critical('Number of retries exceeded to connect to DB with the error:')
        retry_err.reraise()
        sys.exit(1)
    if live:
        sql='INSERT INTO king_pilot.daily_raw_bt VALUES %s'
        with conn:
            with conn.cursor() as cur:
                   execute_values(cur, sql, to_insert)  
    else:
        sql='INSERT INTO bluetooth.raw_data VALUES %s'
        with conn:
            with conn.cursor() as cur:
                   execute_values(cur, sql, to_insert)  
    conn.close()


def get_wsdl_client(wsdlfile, direct=None, live=False):
    """
    Create a zeep Client from the WSDL Url provided
        :param wsdlfile: 
            URL to WSDL file
        :param direct=None: 
            If True, suppresses HTTPS_PROXY env variable
    """ 
    #workaround for insecure SSL certificate
    session = Session()
    session.verify = False
    if direct is True:
        session.proxies = {'https': '',
                           'http': ''}
        os.environ['HTTPS_PROXY'] = ''
    transport = Transport(session=session)
    try:
        blip = Client(wsdlfile, transport=transport)
    except:
        LOGGER.warning('First Client connection failed, trying again')
        time.sleep(10)
        blip = Client(wsdlfile, transport=transport)
    # Create a config object
    config = blip.type_factory('ns0').perUserDataExportConfiguration()
    config.live = live
    config.includeOutliers = True
    # Weird hack to prevent a bug
    # See https://stackoverflow.com/a/46062820/4047679
    for key in config:
        if config[key] is None:
            config[key] = zeep.xsd.SkipValue
    return blip, config

def update_configs(all_analyses, dbset):
    '''
    Syncs configs from blip server with database and returns configs to pull 
    data from. 
    :param all_analyses:
        List of blip configurations
    :param dbset:
        Dictionary to connect to PostgreSQL database
    '''

    try:
        conn = _get_db(dbset)
    except RetryError as retry_err:
        LOGGER.critical('Number of retries exceeded to connect to DB with the error:')
        retry_err.reraise()
    with conn:
        with conn.cursor() as curs:
            curs.execute('''TRUNCATE bluetooth.all_analyses_day_old;
                            INSERT INTO bluetooth.all_analyses_day_old 
							SELECT * FROM bluetooth.all_analyses;''')
        
    analyses_pull_data = {}
    for report in all_analyses:
        outcomes_arr = json.dumps([outcome.__json__() for outcome in report.outcomes])
        routePoints_arr = json.dumps([route_point.__json__() for route_point in report.routePoints])

        row = dict(device_class_set_name=report.deviceClassSetName,
                   analysis_id=report.id,
                   minimum_point_completed=json.dumps(
                        report.minimumPointCompleted.__json__()),
                   outcomes= outcomes_arr,
                   report_id=report.reportId,
                   report_name=report.reportName,
                   route_id=report.routeId,
                   route_name=report.routeName,
                   route_points=routePoints_arr)

        #If upsert fails, log error and continue, don't add analysis to analyses to pull
        try:
            upsert_sql = '''INSERT INTO bluetooth.all_analyses (device_class_set_name, analysis_id, 
                                                                minimum_point_completed, outcomes, report_id, report_name, 
                                                                route_id, route_name, route_points)
                            VALUES (%(device_class_set_name)s, %(analysis_id)s, %(minimum_point_completed)s, %(outcomes)s, %(report_id)s, %(report_name)s, %(route_id)s, %(route_name)s, %(route_points)s)
                            ON CONFLICT (analysis_id)
                            DO UPDATE SET (device_class_set_name, minimum_point_completed, outcomes, report_id, report_name, route_id, route_name, route_points)
                                            = (EXCLUDED.device_class_set_name,
                                               EXCLUDED.minimum_point_completed,
                                               EXCLUDED.outcomes ,
                                               EXCLUDED.report_id,
                                               EXCLUDED.report_name,
                                               EXCLUDED.route_id ,
                                               EXCLUDED.route_name ,
                                               EXCLUDED.route_points)
                            RETURNING analysis_id, report_name, pull_data; 
                        '''
            with conn:
                with conn.cursor() as cur:  
                    cur.execute(upsert_sql, row)
                    upserted = cur.fetchone()
            analyses_pull_data[upserted[0]] = {'pull_data': upserted[2],
                                                           'report_name': upserted[1]}
        except IntegrityError as err:
            LOGGER.error(err)

    conn.close()
    analyses_to_pull = {analysis_id: analysis for (
                                                analysis_id, analysis) in analyses_pull_data.items() if analysis['pull_data']}

    return analyses_to_pull

def move_data(dbset):
    try:
        try:
            conn = _get_db(dbset)
        except RetryError as retry_err:
            LOGGER.critical('Number of retries exceeded to connect to DB with the error:')
            retry_err.reraise()
        ## BT move raw data
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT bluetooth.move_raw_data();")
                query = cur.fetchone()
        if query[0] != 1:
            conn.rollback()
            raise DatabaseError('bluetooth.move_raw_data did not complete successfully') 
        ## King pilot 
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT king_pilot.load_bt_data();")
                query = cur.fetchone()    
        if query[0] != 1:
            conn.rollback()
            raise DatabaseError('king_pilot.load_bt_data did not complete successfully') 
        ## Truncate and delete if successful 
        with conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE bluetooth.raw_data;")
                cur.execute("DELETE FROM king_pilot.daily_raw_bt WHERE measured_timestamp < now()::DATE;")
                
    except DatabaseError as dberr:
        LOGGER.error(dberr)
        conn.rollback()
    except IntegrityError:
        LOGGER.critical('Moving data failed due to violation of a constraint. Data will have to be moved manually')
    finally:
        conn.close()

def load_config(dbsetting):
    config = configparser.ConfigParser()
    config.read(dbsetting)
    dbset = config['DBSETTINGS']
    api_settings = config['API']
    return dbset, api_settings

def main(dbsetting: 'path/to/config.cfg' = None,
         years: '[[YYYYMMDD, YYYYMMDD]]' = None,
         direct: bool = None,
         live: bool = False,
         analysis: List[int] = []):
    """
    Main method. Connect to blip WSDL Client, update analysis configurations,
    then pull data for specified dates
        :param dbsetting=None: 
            Path to the config.cfg file
        :param years=None: 
            List of [[YYYYMMDD, YYYYMMDD]] pairs
        :param direct=None: 
            Specify to ignore the HTTPS_PROXY environment variable.
    """

    dbset, api_settings = load_config(dbsetting)

    # Access the API using zeep (Why do we need to sleep the connection when that function exist)
    LOGGER.info('Fetching config from blip server')
    blip, config = get_wsdl_client(api_settings['WSDLfile'], direct, live)

    try: 
        conn = _get_db(dbset) 
    except RetryError as retry_err: 
        LOGGER.critical('Number of retries exceeded to connect to DB with the error:') 
        retry_err.reraise() 
        sys.exit(1) 

    if live:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT analysis_id, report_name from king_pilot.bt_segments INNER JOIN bluetooth.all_analyses USING(analysis_id)")
            routes_to_pull = {analysis_id: dict(report_name = report_name) for analysis_id, report_name in cur.fetchone()}

    else:
        #Querying data that's been further processed overnight
        if not analysis:
            # list of all route segments
            all_analyses = blip.service.getExportableAnalyses(api_settings['un'],
                                                            api_settings['pw'])
            LOGGER.info('Updating route configs')
            routes_to_pull = update_configs(all_analyses, dbset)

        else:
            LOGGER.info('Fetching info on the following analyses from the database: %s', analysis)
            sql = '''WITH analyses AS (SELECT unnest(%(analysis)s::bigint[]) AS analysis_id)
                    SELECT analysis_id, report_name FROM bluetooth.all_analyses INNER JOIN analyses USING(analysis_id)'''
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql, {'analysis':analysis})
                    routes_to_pull = {analysis_id: dict(report_name = report_name) for analysis_id, report_name in cur.fetchall()}

        date_to_process = None

    if years is None and live:
        date_to_process = datetime.datetime.now().replace(minute=0, second=0, microsecond=0)
        years = {date_to_process.year: [date_to_process.month]}
    elif years is None:
        # Use today's day to determine month to process
        date_to_process = date.today() + relativedelta(days=-1)
        years = {date_to_process.year: [date_to_process.month]}
    else:
        # Process and test whether the provided yyyymm is accurate
        years = validate_multiple_yyyymmdd_range(years)

    for year in years:
        for (analysis_id, analysis), month in product(routes_to_pull.items(), years[year]):
            if date_to_process is None:
                days = list(years[year][month])
                LOGGER.info('Reading from: %s y: %s m: %s days: %s-%s',
                            analysis['report_name'],
                            str(year),
                            str(month),
                            str(days[0]),
                            str(days[-1]))
                config.startTime = datetime.datetime(year, month, days[0], 0, 0, 0)
            elif live:
                LOGGER.info('Reading from: %s at %s ',
                            analysis['report_name'],
                            date_to_process.strftime('%Y-%m-%d %H:00'))
                config.startTime = date_to_process + relativedelta(hours=-1)
                config.endTime = date_to_process
            else:
                days = [date_to_process]

                config.startTime = datetime.datetime.combine(date_to_process,
                                                             datetime.datetime.min.time())
                LOGGER.info('Reading from: %s for %s ',
                            analysis['report_name'],
                            date_to_process.strftime('%Y-%m-%d'))

            config.analysisId = analysis_id
            objectList = []
            if live:
                objectList.extend(get_data_for_config(blip,
                                                      api_settings['un'],
                                                      api_settings['pw'],
                                                      config))
            else:
                ks = [0, 1, 2, 3]
                for _, k in product(days, ks):
                    if k == 0:
                        config.endTime = config.startTime + \
                            datetime.timedelta(hours=8)
                    elif k == 1:
                        config.endTime = config.startTime + \
                            datetime.timedelta(hours=6)
                    elif k == 2 or k == 3:
                        config.endTime = config.startTime + \
                            datetime.timedelta(hours=5)

                    objectList.extend(get_data_for_config(blip,
                                                        api_settings['un'],
                                                        api_settings['pw'],
                                                        config))

                    if k == 0:
                        config.startTime = config.startTime + \
                            datetime.timedelta(hours=8)
                    elif k == 1:
                        config.startTime = config.startTime + \
                            datetime.timedelta(hours=6)
                    elif k == 2 or k == 3:
                        config.startTime = config.startTime + \
                            datetime.timedelta(hours=5)
                    time.sleep(1)
            try:
                insert_data(objectList, dbset, live)
            except OSError as ose:
                LOGGER.error('Inserting data failed')
                LOGGER.error(ose)
            except ValueError as valu:
                LOGGER.error('Unsupported Value in insert')
                LOGGER.error(valu)
            except IntegrityError:
                LOGGER.warning('Insert violated table constraints, likely duplicate data')

    if not live:
        LOGGER.info('Moving raw data to observations.')
        
        move_data(dbset)
        

    LOGGER.info('Processing Complete.')


if __name__ == '__main__':

    FORMAT = '%(asctime)s %(name)-2s %(levelname)-2s %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)

    ARGS = parse_args(sys.argv[1:])
    if ARGS.years is None:
        DATE_TO_PROCESS = date.today() + relativedelta(days=-1)
        LOGGER.info('Pulling data from %s', DATE_TO_PROCESS)
    else:
        LOGGER.info('Pulling data from %s to %s',
                    ARGS.years[0][0], ARGS.years[-1][-1])
    try:
        main(**vars(ARGS))
    except Exception as exc:
        LOGGER.critical(traceback.format_exc())