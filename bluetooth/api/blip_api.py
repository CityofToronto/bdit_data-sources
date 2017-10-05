import argparse
import calendar
import configparser
import datetime
import logging
import os
import sys
import time
import traceback
from datetime import date
from itertools import product

import zeep
from dateutil.relativedelta import relativedelta
from requests import RequestException, Session
from zeep import Client
from zeep.transports import Transport

#Suppress HTTPS Warnings
import urllib3
from parsing_utilities import validate_multiple_yyyymmdd_range
from pg import DB

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

LOGGER = logging.getLogger(__name__)

def parse_args(args, prog=None, usage=None):
    '''Parser for the command line arguments'''
    PARSER = argparse.ArgumentParser(description='Pull data from blip API and '
                                                 'send to database',
                                     prog=prog, usage=usage)
    PARSER.add_argument("-y", "--years", nargs=2, action='append',
                        help="Range of dates (YYYYMMDD) to operate over "
                        "from startdate to enddate, else defaults to "
                        "previous day. ",
                        metavar=('YYYYMMDD', 'YYYYMMDD'))
    PARSER.add_argument("-d", "--dbsetting",
                        default='config.cfg',
                        help="Filename with connection settings to the database "
                        "(default: opens %(default)s)")
    PARSER.add_argument("--direct",
                        action='store_true',
                        help="Use DIRECT proxy if using from workstation")
    
    return PARSER.parse_args(args)

def get_data_for_config(blip, un, pw, config):
    
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

def insert_data(data, dbset):
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
        #convert each observation dictionary into a tuple row for inserting
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
    # workaround for insecure SSL certificate
    session = Session()
    session.verify = False
    if direct is True:
        session.proxies = {'https':'',
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
    #Weird hack to prevent a bug
    #See https://stackoverflow.com/a/46062820/4047679
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
    updated_num= 0 
    for report in all_analyses:
        report.outcomes = [outcome.__json__() for outcome in report.outcomes]
        report.routePoints = [route_point.__json__() for route_point in report.routePoints]
        row = dict(device_class_set_name = report.deviceClassSetName,
                   id = report.id,
                   minimum_point_completed = db.encode_json(report.minimumPointCompleted.__json__()),
                   outcomes = report.outcomes, 
                   report_id = report.reportId, 
                   report_name = report.reportName,
                   route_id = report.routeId,
                   route_name = report.routeName,
                   route_points = report.routePoints)
        upserted = db.upsert('bluetooth.all_analyses', row, pull_data='included.pull_data')
        existing_row = existing_analyses.get(upserted['id'], None)
        if existing_row is None:
            insert_num += insert_num
        elif dict(existing_row._asdict(), id=upserted['id']) != upserted:
            updated_num += updated_num
        analyses_pull_data[upserted['id']] = {'pull_data': upserted['pull_data'],
                                        'report_name' : upserted['report_name']}
        
    if insert_num > 0:
        LOGGER.info('%s new report configurations uploaded', insert_num)
    if updated_num > 0:
        LOGGER.info('%s new report configurations uploaded', insert_num)
            
    db.close()
    
    analyses_to_pull = {analysis_id:analysis for (analysis_id, analysis) in analyses_pull_data.items() if analysis['pull_data'] == True}
    return analyses_to_pull


def main(dbsetting = None, years = None, direct = None):
    CONFIG = configparser.ConfigParser()
    CONFIG.read(dbsetting)
    dbset = CONFIG['DBSETTINGS']
    api_settings = CONFIG['API']
    
    # Access the API using zeep 
    LOGGER.info('Fetching config from blip server')
    blip, config = get_wsdl_client(api_settings['WSDLfile'], direct)

    # list of all route segments
    allAnalyses = blip.service.getExportableAnalyses(api_settings['un'],
                                                     api_settings['pw'])
    LOGGER.info('Updating route configs')
    routes_to_pull = update_configs(allAnalyses, dbset)
    
    date_to_process = None

    if years is None:
        #Use today's day to determine month to process
        date_to_process = date.today() + relativedelta(days=-1)
        years = {date_to_process.year: [date_to_process.month]}

    else:
        #Process and test whether the provided yyyymm is accurate
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
                days=[1]
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
                    config.endTime = config.startTime + datetime.timedelta(hours = 8)
                elif k == 1:
                    config.endTime = config.startTime + datetime.timedelta(hours = 6)
                elif k == 2 or k == 3:
                    config.endTime = config.startTime + datetime.timedelta(hours = 5)
                    
                
                objectList.extend(get_data_for_config(blip,
                                                      api_settings['un'],
                                                      api_settings['pw'],
                                                      config))
    
                if k == 0:
                    config.startTime = config.startTime + datetime.timedelta(hours = 8)
                elif k == 1:
                    config.startTime = config.startTime + datetime.timedelta(hours = 6)
                elif k == 2 or k == 3:
                    config.startTime = config.startTime + datetime.timedelta(hours = 5)
                time.sleep(1)
    
            insert_data(objectList, dbset)
    LOGGER.info('Processing Complete.')
            
if __name__ == '__main__':
    
    FORMAT = '%(asctime)s %(name)-2s %(levelname)-2s %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    
    args = parse_args(sys.argv[1:])
    if args.years is None:
        date_to_process = date.today() + relativedelta(days=-1)
        LOGGER.info('Pulling data from %s', date_to_process)
    else:
        LOGGER.info('Pulling data from %s to %s',
                    args.years[0][0], args.years[-1][-1])
    try:
        main(**vars(args))
    except Exception as exc:
        LOGGER.critical(traceback.format_exc())
