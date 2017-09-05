import calendar
import datetime
import datetime.date as date
import time
import logging
import configparser
import argparse
import sys

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

def selectDate():
    dateStr = str(input('Input a date [YYYY-MM-DD]: '))
    return(datetime.datetime.strptime(dateStr, '%Y-%m-%d'))

def get_data_for_config(blip, un, pw, config):
    
    try:
        data = blip.service.exportPerUserData(un, pw, config) 
    except RequestException as err: 
        LOGGER.error(err)            
        time.sleep(30)
        data = blip.service.exportPerUserData(un, pw, config) 
    except Exception: 
        LOGGER.error('In the error handler (SUDS error)...')            
        time.sleep(15)
        data = blip.service.exportPerUserData(un, pw, config) 
    return data

def insertDF(df, dbset):
    try:    
        LOGGER.info('Uploading to PostgreSQL' + ', ' + datetime.datetime.now().strftime('%H:%M:%S'))     
        #TODO Fix this
        engine = DB.create_engine(**dbset)
        df.to_sql('raw_data',engine, schema = 'bluetooth', if_exists = 'append', index = False)
    except:
        LOGGER.info('Server Connection Error Handler' + ', '+ datetime.datetime.now().strftime('%H:%M:%S'))
        time.sleep(60)                    
        insertDF(df)
        
def get_wsdl_client(wsdlfile):
    # workaround for insecure SSL certificate
    session = Session()
    session.verify = False
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

if __name__ == '__main__':
    
    FORMAT = '%(asctime)s %(name)-2s %(levelname)-2s %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    

    ARGS = parse_args(sys.argv[1:])
    
    CONFIG = configparser.ConfigParser()
    CONFIG.read(ARGS.dbsetting)
    dbset = CONFIG['DBSETTINGS']
    api_settings = CONFIG['API']
    
    # Access the API using zeep 
    LOGGER.info('Going in!')
    blip, config = get_wsdl_client(api_settings['WSDLfile'])

    # list of all route segments
    allAnalyses = blip.service.getExportableAnalyses(api_settings['un'],
                                                     api_settings['pw'])


    if ARGS.years is None:
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
    
            try:
                x = pd.DataFrame([Client.dict(item) for sublist in objectList for item in sublist])
                x = x.rename(columns={"userId" : "user_id", "analysisId" : "analysis_id", "measuredTime" : "measured_time", "measuredTimeNoFilter" : "measured_time_no_filter", \
                "startPointNumber" : "startpoint_number", "startPointName" : "startpoint_name", "endPointNumber" : "endpoint_number", "endPointName" : "endpoint_name", \
                "measuredTimeTimestamp" : "measured_timestamp", "outlierLevel" : "outlier_level", "deviceClass" : "device_class"})
                x.measured_timestamp = pd.to_datetime(x.measured_timestamp, infer_datetime_format=True, format='%m/%d/%Y  %H:%M:%S %p')
                insertDF(x, dbset)
            except:
                LOGGER.error('FAILED: ' + str(allAnalyses[j].reportName) + ' y: ' + str(year) + ' m: ' + str(month))
