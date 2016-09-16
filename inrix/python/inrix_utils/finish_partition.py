#!/usr/bin/python3
'''Finish partitioning tables in the Inrix database.'''
import logging
from time import sleep
from psycopg2 import connect, OperationalError
from psycopg2.extensions import AsIs
from utils import get_yyyymm, get_yyyymmdd, try_connection

def _partition_table(tableyyyymm, startdate, logger, cursor, timecol):
    '''Add check constraints on the inrix.raw_data partitioned table ending with yyyymm.'''
    logger.info('Adding check constraints on table %s', tableyyyymm)
    cursor.execute("ALTER TABLE %(table)s ADD CHECK (%(timecol)s >= DATE %(startdate)s "
                   "AND %(timecol)s < DATE %(startdate)s + INTERVAL '1 month')"
                   , {'table':AsIs(tableyyyymm), 'startdate':startdate,
                   'timecol':AsIs(timecol)})

def partition_tables(years, dbset, logger, table, timecol):
    '''Add check constraints for a series of tables based on the years dictionary \
    and the dbset database connection.'''

    con, cursor = try_connection(logger, dbset, autocommit=True)

    for year in years:
        for month in years[year]:
            tableyyyymm = table + get_yyyymm(year, month)
            startdate = get_yyyymmdd(year, month)

            while True:
                try:
                    _partition_table(tableyyyymm, startdate, logger, cursor, timecol)
                except OperationalError as oe:
                    logger.error(oe)
                    con, cursor = try_connection(logger, dbset, autocommit=True)
                else:
                    break

    logger.info('Partitioning complete, connection to %s database %s closed',
                dbset['host'],
                dbset['database'])
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
    partition_tables(YEARS, dbsetting, LOGGER, table='inrix.raw_data')
