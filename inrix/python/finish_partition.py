#!/usr/bin/python3
'''Finish partitioning tables in the Inrix database.'''
import logging
from psycopg2 import connect

def get_yyyymm(yyyy, mm):
    '''Combine integer yyyy and mm into a string yyyymm.'''
    if mm < 10:
        return str(yyyy)+'0'+str(mm)
    else:
        return str(yyyy)+str(mm)

def get_yyyy_mm_dd(yyyy, mm):
    '''Combine integer yyyy and mm into a string date yyyy-mm-dd.'''
    if mm < 10:
        return str(yyyy)+'-0'+str(mm)+'-01'
    else:
        return str(yyyy)+'-'+str(mm)+'-01'

    
def _partition_table(yyyymm, logger, con, cursor):
    '''Add check constraints on the inrix.raw_data partitioned table ending with yyyymm.'''
    table = 'inrix.raw_data'+yyyymm
    logger.info('Adding check constraits on table %s', table)
    cursor.execut('ALTER TABLE {table} ADD CHECK (tx >= DATE {startdate} AND tx < DATE {startdate} + INTERVAL \'1 month\')'
                  .format(table=table, startdate=startdate))


def partition_tables(years, dbset, logger):
    '''Add check constraints for a series of tables based on the years dictionary \
    and the dbset database connection.'''

    logger.info('Connecting to host:%s database: %s with user %s',
                dbset['database'],
                dbset['host'],
                dbset['user'])
    con = connect(database=dbset['database'],
                  host=dbset['host'],
                  user=dbset['user'],
                  password=dbset['password'])
    #Necessary for index building
    con.autocommit=True
    
    cursor = con.cursor()

    for year in years:
        for month in years[year]:
            yyyymm = get_yyyymm(year, month)
            startdate = get_yyyy_mm_dd(year, month)
            _partition_table(yyyymm, startdate, logger, con, cursor)

    con.close()

if __name__ == "__main__":
    #For initial run, creating years and months of available data as a python dictionary
    YEARS = {"2012":range(7, 13),
             "2013":range(1, 7),
             "2016":range(1, 7),
             "2014":range(1, 13),
             "2015":range(1, 13)}
    #Configure logging
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    LOGGER = logging.getLogger(__name__)
    from dbsettings import dbsetting
    partition_tables(YEARS, dbsetting, LOGGER)
