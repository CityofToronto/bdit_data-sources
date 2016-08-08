#!/usr/bin/python3
'''Converting timestamps in the Inrix database to local time.'''
import logging
from psycopg2 import connect

def get_yyyymm(yyyy, mm):
    '''Combine integer yyyy and mm into a string yyyymm.'''
    if mm < 10:
        return str(yyyy)+'0'+str(mm)
    else:
        return str(yyyy)+str(mm)

def _update_table(yyyymm, logger, con, cursor):
    '''Update the inrix.raw_data partitioned table with yyyymm
    to the local timezone. '''
    table = 'inrix.raw_data'+yyyymm
    logger.info('Updating timestamps on table %s', table)
    cursor.execute("UPDATE {table} "
                   "SET tx = (tx AT TIME ZONE 'UTC') AT TIME ZONE 'America/Toronto' ;"
                   .format(table=table))
    con.commit()

def update_tables(years, dbset, logger):
    '''Update a series of tables based on the years dictionary \
    and the dbset database connection.'''

    logger.info('Connecting to host:%s database: %s with user %s',
                dbset['database'],
                dbset['host'],
                dbset['user'])
    con = connect(database=dbset['database'],
                  host=dbset['host'],
                  user=dbset['user'],
                  password=dbset['password'])
    cursor = con.cursor()

    for year in years:
        for month in years[year]:
            yyyymm = get_yyyymm(year, month)
            _update_table(yyyymm, logger, con, cursor)

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
    update_tables(YEARS, dbsetting, LOGGER)
