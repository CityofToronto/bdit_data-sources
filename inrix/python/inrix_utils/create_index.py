#!/usr/bin/python3
'''Create indexes for each table in the Inrix database.'''
import logging
from psycopg2.extensions import AsIs
from utils import try_connection, execute_function

SQL_FUNCTIONS = {'score': "SELECT inrix.create_raw_score_idx(%(tablename)s)",
                 'tmc': "SELECT inrix.create_raw_tmc_idx(%(tablename)s)",
                 'timestamp': "SELECT inrix.create_raw_tx_idx(%(tablename)s)"}

def _create_index(logger, cursor, *, table, index, **kwargs):
    '''Create tx index on the inrix.raw_data partitioned table with table.'''
    logger.info('Creating %s index', index)
    sql = SQL_FUNCTIONS[index]
    cursor.execute(sql, {'tablename':table})

def _analyze_table(logger, cursor, *, table, **kwargs):
    '''Analyze inrix.raw_data partitioned table with table.'''
    logger.info('Analyzing table %s', table)
    cursor.execute("ANALYZE inrix.%(tablename)s", {'tablename':AsIs(table)})

def index_tables(yyyymm, logger, cursor, dbset, *, indexes=['score','tmc','timestamp'], **kwargs):
    '''Create indexes for a series of tables based on the years dictionary \
    and the dbset database connection.'''

    table = 'raw_data'+yyyymm
    logger.info('Creating indexes on table %s', table)

    for index in indexes:
        #Execute the specified "CREATE INDEX" sql function on the specified table
        execute_function(_create_index, logger, cursor, dbset, table=table, index=index, autocommit=True)

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
    con, cursor = try_connection(LOGGER, dbset, autocommit=True)
    index_tables(YEARS, LOGGER, cursor, dbsettings)
