import calendar
import datetime
from datetime import date 
import time
import logging
import configparser
import argparse
import sys
import traceback

import zeep
from zeep import Client
from zeep.transports import Transport
from requests import Session, RequestException

from pg import DB
from parsing_utilities import validate_multiple_yyyymm_range
from dateutil import relativedelta


LOGGER = logging.getLogger(__name__)

ALL_IND = [0,1,4,6,7,8,9,10,11,12,13,14,15,16,58,59,60,61,70,71,74,75,76,77,78,79,80,81,82,83,84,85,86,87,95,96,97,98,99,149,150,151,181,182,183]
ALL_IND.extend(list(range(102,131)))

def parse_args(args, prog = None, usage = None):
    '''Parser for the command line arguments'''
    PARSER = argparse.ArgumentParser(description='Pull data from blip API and '
                                                 'send to database',
                                     prog=prog, usage=usage)
    PARSER.add_argument("-y", "--years", nargs=2, action='append',
                                 help="Range of months (YYYYMM) to operate over "
                                 "from startdate to enddate, else defaults to "
                                 "previous month",
                                 metavar=('YYYYMM', 'YYYYMM'))
    PARSER.add_argument("-d", "--dbsetting",
                        default='default.cfg',
                        help="Filename with connection settings to the database "
                        "(default: opens %(default)s)")
    return PARSER.parse_args(args)

def get_data_for_config(blip, un, pw, config):
    
    try:
        data = blip.service.exportPerUserData(un, pw, config) 
    except RequestException as err: 
        LOGGER.error(err)            
        time.sleep(30)
        data = blip.service.exportPerUserData(un, pw, config) 
    except Exception as exc: 
        LOGGER.error(exc)            
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
               dic["measuredTimeNoFilter"],dic["startPointNumber"], 
               dic["startPointName"],dic["endPointNumber"], dic["endPointName"],
               dic["measuredTimeTimestamp"],dic["outlierLevel"], dic["cod"], 
               dic["deviceClass"])
        to_insert.append(row)
    
    db = DB(**dbset)
    db.inserttable('bluetooth.raw_data', to_insert)
    db.close()
    
        
def get_wsdl_client(wsdlfile):
    # workaround for insecure SSL certificate
    session = Session()
    #session.verify = False
    session.proxies = {'https':'https://137.15.73.132:8080'}
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

def main(dbsetting = None, years = None):
    CONFIG = configparser.ConfigParser()
    CONFIG.read(dbsetting)
    dbset = CONFIG['DBSETTINGS']
    api_settings = CONFIG['API']
    
    # Access the API using zeep 
    LOGGER.info('Going in!')
    blip, config = get_wsdl_client(api_settings['WSDLfile'])

    # list of all route segments
    allAnalyses = blip.service.getExportableAnalyses(api_settings['un'],
                                                     api_settings['pw'])


    if years is None:
        #Use today's day to determine month to process
        last_month = date.today() + relativedelta(months=-1)
        years = {last_month.year: last_month.month}

    else:
        #Process and test whether the provided yyyymm is accurate
        years = validate_multiple_yyyymm_range(ARGS.years, 'month')

    
    
    for year in years:
        for j, month in zip(ALL_IND, years[year]):
            ndays = calendar.monthrange(year, month)[1]
            # ndays = 10
            LOGGER.info('Reading from: ' + str(allAnalyses[j].reportName) + ' y: ' + str(year) + ' m: ' + str(month))
            config.analysisId = allAnalyses[j].id
            config.startTime = datetime.datetime(year, month, 1, 0, 0, 0)
    
            days = list(range(ndays))
            ks = [0, 1, 2, 3, 4]
            
            objectList = []
            
            for i, k in zip(days, ks):               
                if k == 0:                        
                    config.endTime = config.startTime + datetime.timedelta(hours = 8)
                elif k == 1:
                    config.endTime = config.startTime + datetime.timedelta(hours = 6)
                elif k == 2 or k == 3:
                    config.endTime = config.startTime + datetime.timedelta(hours = 5)
    
                objectList.append(get_data_for_config(blip,
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
            
if __name__ == '__main__':
    
    FORMAT = '%(asctime)s %(name)-2s %(levelname)-2s %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    
    args = parse_args(sys.argv[1:])
    
    try:
        main(**vars(args))
    except Exception as exc:
        LOGGER.critical(traceback.format_exc())