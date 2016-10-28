#!/usr/bin/python3
'''Finish partitioning tables in the Inrix database.'''
import logging
from psycopg2.extensions import AsIs
from utils import get_yyyymm, get_yyyymmdd, try_connection, execute_function

def partition_table(logger, cursor, *, tablename, yyyymm, startdate, timecol = 'tx', **kwargs):
    '''Add check constraints on the inrix.raw_data partitioned table ending with yyyymm.'''
    tableyyyymm = tablename + yyyymm
    logger.info('Adding check constraints on table %s', tableyyyymm)
    cursor.execute("ALTER TABLE %(table)s ADD CHECK (%(timecol)s >= DATE %(startdate)s "
                   "AND %(timecol)s < DATE %(startdate)s + INTERVAL '1 month')"
                   , {'table':AsIs(tableyyyymm), 'startdate':startdate,
                   'timecol':AsIs(timecol)})



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
