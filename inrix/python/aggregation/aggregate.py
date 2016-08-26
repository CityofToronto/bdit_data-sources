#!/usr/bin/python3
'''Aggregrates Inrix data to 15 minute bins'''
import logging
from psycopg2 import connect, OperationalError, InterfaceError
from psycopg2.extensions import AsIs
from time import sleep

def get_yyyymm(yyyy, mm):
    '''Combine integer yyyy and mm into a string yyyymm.'''
    if mm < 10:
        return str(yyyy)+'0'+str(mm)
    else:
        return str(yyyy)+str(mm)

def _agg_table(yyyymm, logger, con, cursor):
    '''Aggregate data from the inrix.raw_data partitioned table with yyyymm
    and insert it in inrix.agg_extract_hour.'''
    rawtable = 'inrix.raw_data'+yyyymm
    aggtable = 'inrix.agg_extract_hour'+yyyymm
    logger.info('Aggregating table %s', rawtable)
    cursor.execute("CREATE TABLE IF NOT EXISTS %(aggtable)s ()INHERITS(inrix.agg_extract_hour);", {'aggtable':AsIs(aggtable)})
    cursor.execute("INSERT INTO %(aggtable)s "
                   "SELECT "
                   "tmc, "
                   "extract(hour from tx)*10 + trunc(extract(minute from tx)/15)+1 AS time_15_continuous, "
                   "tx::DATE as dt, "
                   "COUNT(speed) AS cnt, "
                   "AVG(speed) AS avg_speed "
                   "FROM %(rawtable)s "
                   "WHERE score = 30 "
                   "GROUP BY tmc, tx::date, time_15_continuous",
                   {'aggtable':AsIs(aggtable), 'rawtable': AsIs(rawtable)})
    con.commit()

def agg_tables(years, dbset, logger):
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
                    _agg_table(yyyymm, logger, con, cursor)
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
