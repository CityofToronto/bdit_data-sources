"""blip_api.py

Script to pull Bluetooth data from the Blip api. 

main defaults to getting the previous day's data 

"""

import argparse
import configparser
import datetime
from datetime import date
import logging
import os
import sys
import time
import traceback
from itertools import product

import zeep
from zeep import Client
from zeep.transports import Transport
from dateutil.relativedelta import relativedelta
from requests import RequestException, Session

import urllib3
from parsing_utilities import validate_multiple_yyyymmdd_range
from email_notifications import send_email
from pg import DB

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
    parser.add_argument("-d", "--dbsetting",
                        default='config.cfg',
                        help="Filename with connection settings to the database "
                        "(default: opens %(default)s)")
    parser.add_argument("--direct",
                        action='store_true',
                        help="Use DIRECT proxy if using from workstation")

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


def insert_data(data: list, dbset: dict):
    '''
    Upload data to the database

    :param data:
        List of dictionaries, gets converted to list of tuples
    :param dbset:
        DB settings passed to Pygresql to create a connection 
    '''

    LOGGER.info('Uploading to PostgreSQL')
    to_insert = []
    for dic in data:
        # convert each observation dictionary into a tuple row for inserting
        row = (dic["userId"], dic["analysisId"], dic["measuredTime"],
               dic["measuredTimeNoFilter"], dic["startPointNumber"],
               dic["startPointName"], dic["endPointNumber"], dic["endPointName"],
               dic["measuredTimeTimestamp"], dic["outlierLevel"], dic["cod"],
               dic["deviceClass"])
        to_insert.append(row)

    db = DB(**dbset)
    db.inserttable('bluetooth.raw_data', to_insert)
    db.close()


def get_wsdl_client(wsdlfile, direct=None):
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
    config.live = False
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

    db = DB(**dbset)
    existing_analyses = db.get_as_dict('bluetooth.all_analyses')
    analyses_pull_data = {}
    insert_num = 0
    updated_num = 0
    upserted_analyses = []
    for report in all_analyses:
        report.outcomes = [outcome.__json__() for outcome in report.outcomes]
        report.routePoints = [route_point.__json__()
                              for route_point in report.routePoints]
        row = dict(device_class_set_name=report.deviceClassSetName,
                   id=report.id,
                   minimum_point_completed=db.encode_json(
                       report.minimumPointCompleted.__json__()),
                   outcomes=report.outcomes,
                   report_id=report.reportId,
                   report_name=report.reportName,
                   route_id=report.routeId,
                   route_name=report.routeName,
                   route_points=report.routePoints)
        upserted = db.upsert('bluetooth.all_analyses', row,
                             pull_data='included.pull_data')
        existing_row = existing_analyses.get(upserted['id'], None)
        if existing_row is None:
            insert_num += insert_num
            upserted_analyses.append(
                (report.id, report.reportName, report.routeName))
        elif dict(existing_row._asdict(), id=upserted['id']) != upserted:
            updated_num += updated_num
            upserted_analyses.append(
                (report.id, report.reportName, report.routeName))
        analyses_pull_data[upserted['id']] = {'pull_data': upserted['pull_data'],
                                              'report_name': upserted['report_name']}

    if insert_num > 0:
        LOGGER.info('%s new report configurations uploaded', insert_num)
    if updated_num > 0:
        LOGGER.info('%s new report configurations uploaded', insert_num)

    db.close()

    analyses_to_pull = {analysis_id: analysis for (
        analysis_id, analysis) in analyses_pull_data.items() if analysis['pull_data']}
    return analyses_to_pull, upserted_analyses


def email_upserted_analyses(subject: str, to: str, upserted_analyses: list):
    """
    Email updated or new analysis configurations
        :param subject: 
            Email subject line
        :param to: 
            String of addresses to send to
        :param upserted_analyses: 
            A list containg tuples of (report.id, report.reportName, report.routeName)
    """

    message = ''
    for analysis in upserted_analyses:
        message += 'Route id: {route_id}, '\
                   'Report Name: {report_name}, '\
                   'Route Name: {route_name}'.format(route_id=analysis[0],
                                                     report_name=analysis[1],
                                                     route_name=analysis[2])
        message += "\n"
    sender = "Blip API Script"

    send_email(to, sender, subject, message)


def main(dbsetting: 'path/to/config.cfg' = None,
         years: '[[YYYYMMDD, YYYYMMDD]]' = None,
         direct: bool = None):
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

    config = configparser.ConfigParser()
    config.read(dbsetting)
    dbset = config['DBSETTINGS']
    api_settings = config['API']
    email_settings = config['EMAIL']

    # Access the API using zeep
    LOGGER.info('Fetching config from blip server')
    blip, config = get_wsdl_client(api_settings['WSDLfile'], direct)

    # list of all route segments
    all_analyses = blip.service.getExportableAnalyses(api_settings['un'],
                                                      api_settings['pw'])
    LOGGER.info('Updating route configs')
    routes_to_pull, upserted_analyses = update_configs(all_analyses, dbset)

    if len(upserted_analyses) > 0:
        email_upserted_analyses(email_settings['subject'],
                                email_settings['to'],
                                upserted_analyses)

    date_to_process = None

    if years is None:
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
                config.startTime = datetime.datetime(year, month, 1, 0, 0, 0)
            else:
                days = [1]
                config.startTime = datetime.datetime.combine(date_to_process,
                                                             datetime.datetime.min.time())
                LOGGER.info('Reading from: %s for %s ',
                            analysis['report_name'],
                            date_to_process.strftime('%Y-%m-%d'))

            config.analysisId = analysis_id

            ks = [0, 1, 2, 3]

            objectList = []

            for i, k in product(days, ks):
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

            insert_data(objectList, dbset)
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
