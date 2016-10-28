#!/usr/bin/python3
'''Create indexes for each table in the Inrix database.'''
import logging
from psycopg2.extensions import AsIs
from utils import get_yyyymm, try_connection, execute_function


def _score_index_table(logger, cursor, *, table, **kwargs):
    '''Create score index on the inrix.raw_data partitioned table with table.'''
    logger.info('Creating score index')
    cursor.execute("SELECT inrix.create_raw_score_idx(%(tablename)s)", {'tablename':table})

def _tmc_index_table(logger, cursor, *, table, **kwargs):
    '''Create tmc index on the inrix.raw_data partitioned table with table.'''
    logger.info('Creating tmc index')
    cursor.execute("SELECT inrix.create_raw_tmc_idx(%(tablename)s)", {'tablename':table})

def _tx_index_table(logger, cursor, *, table, **kwargs):
    '''Create tx index on the inrix.raw_data partitioned table with table.'''
    logger.info('Creating timestamp index')
    cursor.execute("SELECT inrix.create_raw_tx_idx(%(tablename)s)", {'tablename':table})

def _analyze_table(logger, cursor, *, table, **kwargs):
    '''Analyze inrix.raw_data partitioned table with table.'''
    logger.info('Analyzing table %s', table)
    cursor.execute("ANALYZE inrix.%(tablename)s", {'tablename':AsIs(table)})

def index_tables(yyyymm, logger, cursor, dbset, *, score=True, tmc=True, tx=True):
    '''Create indexes for a series of tables based on the years dictionary \
    and the dbset database connection.'''

    table = 'raw_data'+yyyymm
    logger.info('Creating indexes on table %s', table)

    if score:
        execute_function(_score_index_table, logger, cursor, dbset, table=table, autocommit=True)

    if tmc:
        execute_function(_tmc_index_table, logger, cursor, dbset, table=table, autocommit=True)

    if tx:
        execute_function(_tx_index_table, logger, cursor, dbset, table=table, autocommit=True)

    execute_function(_analyze_table, logger, cursor, dbset, table=table, autocommit=True)

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
