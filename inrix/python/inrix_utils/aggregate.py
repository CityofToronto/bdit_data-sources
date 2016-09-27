#!/usr/bin/python3
'''Aggregrates Inrix data to 15 minute bins'''
import logging
from time import sleep
from psycopg2 import connect, OperationalError, InterfaceError

def get_yyyymm(yyyy, mm):
    '''Combine integer yyyy and mm into a string yyyymm.'''
    if mm < 10:
        return str(yyyy)+'0'+str(mm)
    else:
        return str(yyyy)+str(mm)

def _agg_table(yyyymm, logger, cursor, **kwargs):
    '''Aggregate data from the inrix.raw_data partitioned table with yyyymm
    and insert it in inrix.agg_extract_hour.'''

    if 'tmcschema' in kwargs:
        logger.info('Aggregating table %s with subset from schema %s table %s',
                    'inrix.raw_data'+yyyymm, kwargs['tmcschema'], kwargs['tmctable'])
        cursor.execute('SELECT inrix.agg_extract_hour_alldata(%(yyyymm)s, '
                    '%(tmcschema)s, %(tmctable)s)',
                    {'yyyymm':yyyymm,
                     'tmcschema':kwargs['tmcschema'],
                     'tmctable':kwargs['tmctable']})
    elif 'alldata' in kwargs:
        logger.info('Aggregating table %s using all data', 'inrix.raw_data'+yyyymm)
        cursor.execute('SELECT inrix.agg_extract_hour_alldata(%(yyyymm)s)', {'yyyymm':yyyymm})
    else:
        logger.info('Aggregating table %s', 'inrix.raw_data'+yyyymm)
        cursor.execute('SELECT inrix.agg_extract_hour(%(yyyymm)s)', {'yyyymm':yyyymm})

    cursor.commit()

def agg_tables(years, dbset, logger, **kwargs):
    '''Update a series of tables based on the years dictionary \
    and the dbset database connection.'''

    logger.info('Connecting to host:%s database: %s with user %s',
                dbset['host'],
                dbset['database'],
                dbset['user'])
    con = connect(database=dbset['database'],
                  host=dbset['host'],
                  user=dbset['user'],
                  password=dbset['password'])
    cursor = con.cursor()

    for year in years:
        for month in years[year]:
            yyyymm = get_yyyymm(year, month)
            #Execution retry loop
            while True:
                try:
                    _agg_table(yyyymm, logger, cursor, **kwargs)
                except (OperationalError, InterfaceError) as oe:
                    logger.error(oe)
                    logger.info('Retrying connection in 2 minutes')
                    sleep(120)
                    try:
                        con = connect(database=dbset['database'],
                                      host=dbset['host'],
                                      user=dbset['user'],
                                      password=dbset['password'])
                        cursor = con.cursor()
                    except (OperationalError, InterfaceError) as oe:
                        pass
                else:
                    break

    con.close()
    logger.info('Processing complete, connection to %s database %s closed',
                dbset['host'],
                dbset['database'])

if __name__ == "__main__":
    #For initial run, creating years and months of available data as a python dictionary
    YEARS = {"2012":range(7, 13),
             "2013":range(1, 13),
             "2011":range(8, 13),
             "2016":range(1, 7),
             "2014":range(1, 13),
             "2015":range(1, 13)}
    #Configure logging
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    LOGGER = logging.getLogger(__name__)
    from dbsettings import dbsetting
    agg_tables(YEARS, dbsetting, LOGGER)
