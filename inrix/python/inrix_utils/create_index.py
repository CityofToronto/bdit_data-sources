#!/usr/bin/python3
'''Create indexes for each table in the Inrix database.'''
import logging
import re
from time import sleep
from psycopg2 import connect, OperationalError
from utils import get_yyyymm, try_connection


def _score_index_table(table, logger, cursor):
    '''Create score index on the inrix.raw_data partitioned table with table.'''
    logger.info('Creating score index')
    cursor.execute("SELECT inrix.create_raw_score_idx(%(tablename)s)", {'tablename':table})

def _tmc_index_table(table, logger, cursor):
    '''Create tmc index on the inrix.raw_data partitioned table with table.'''
    logger.info('Creating tmc index')
    cursor.execute("SELECT inrix.create_raw_tmc_idx(%(tablename)s)", {'tablename':table})

def _tx_index_table(table, logger, cursor):
    '''Create tx index on the inrix.raw_data partitioned table with table.'''
    logger.info('Creating timestamp index')
    cursor.execute("SELECT inrix.create_raw_tx_idx(%(tablename)s)", {'tablename':table})

def _analyze_table(table, logger, cursor):
    '''Analyze inrix.raw_data partitioned table with table.'''
    logger.info('Analyzing table %s', table)
    cursor.execute("SELECT inrix.create_raw_tx_idx(%(tablename)s)", {'tablename':table})

def index_tables(years, dbset, logger, **kwargs):
    '''Create indexes for a series of tables based on the years dictionary \
    and the dbset database connection.'''

    con, cursor = try_connection(logger, dbset, autocommit = True)

    for year in years:
        for month in years[year]:
            yyyymm = get_yyyymm(year, month)

            table = 'raw_data'+yyyymm
            logger.info('Creating indexes on table %s', table)

            #Execution retry loop score
            while 'score' in kwargs:
                try:
                    _score_index_table(table, logger, cursor)
                except OperationalError as oe:
                    logger.error(oe)
                    con, cursor = try_connection(logger, dbset)
                else:
                    break
            #Execution retry loop tmc
            while 'tmc' in kwargs:
                try:
                    _tmc_index_table(table, logger, cursor)
                except OperationalError as oe:
                    logger.error(oe)
                    con, cursor = try_connection(logger, dbset)
                else:
                    break
            #Execution retry loop tx
            while 'tx' in kwargs:
                try:
                    _tx_index_table(table, logger, cursor)
                except OperationalError as oe:
                    logger.error(oe)
                    con, cursor = try_connection(logger, dbset)
                else:
                    break

            while True:
                try:
                    _analyze_table(table, logger, cursor)
                except OperationalError as oe:
                    logger.error(oe)
                    con, cursor = try_connection(logger, dbset)
                else:
                    break

    con.close()
    logger.info('Processing complete, connection to %s database %s closed',
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
    index_tables(YEARS, dbsetting, LOGGER)
