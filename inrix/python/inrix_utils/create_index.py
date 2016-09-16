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
    cursor.execute("SELECT inrix.create_raw_tmc_idx(%(tablename)s)", {'tablename':table})

def index_tables(years, dbset, logger):
    '''Create indexes for a series of tables based on the years dictionary \
    and the dbset database connection.'''

    con, cursor = try_connection(logger, dbset, autocommit = True)

    for year in years:
        for month in years[year]:
            yyyymm = get_yyyymm(year, month)

            table = 'raw_data'+yyyymm
            logger.info('Creating indexes on table %s', table)

            #Execution retry loop score
            while True:
                try:
                    _score_index_table(table, logger, cursor)
                except OperationalError as oe:
                    logger.error(oe)
                    con, cursor = try_connection(logger, dbset)
                else:
                    break
            #Execution retry loop tmc
            while True:
                try:
                    _tmc_index_table(table, logger, cursor)
                except OperationalError as oe:
                    logger.error(oe)
                    con, cursor = try_connection(logger, dbset)
                else:
                    break
            #Execution retry loop tx
            while True:
                try:
                    _tx_index_table(table, logger, cursor)
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
    import argparse
    import json
    import sys

    PARSER = argparse.ArgumentParser(description='Create indexes on raw inrix data tables.')
    #Must have either a year range or pass a JSON file with years.
    YEARS_ARGUMENTS = PARSER.add_mutually_exclusive_group(required=True)
    YEARS_ARGUMENTS.add_argument("-y", "--years", nargs=2,
                                 help="Range of months (YYYYMM) to operate over"
                                 "from startdate to enddate",
                                 metavar=('YYYYMM', 'YYYYMM'))
    YEARS_ARGUMENTS.add_argument("-Y", "--yearsjson", type=json.load,
                                 help="Written dictionary which contains years as key"
                                 "and start/end month like {'2012'=[1,12]}")

    PARSER.add_argument("-d", "--dbsetting",
                        default='default.cfg',
                        help="Filename with connection settings to the database"
                        "(default: opens %(default)s)")

    ARGS = PARSER.parse_args()

    #Configure logging
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    LOGGER = logging.getLogger(__name__)

    import configparser
    CONFIG = configparser.ConfigParser()
    CONFIG.read(ARGS.dbsetting)
    DBSETTING = CONFIG['DBSETTINGS']

    if ARGS.yearsjson:
        try:
            YEARS = _validate_yearsjson(ARGS.yearsjson)
        except ValueError as err:
            LOGGER.critical(str(err))
            sys.exit(2)

    elif ARGS.years:
        try:
            YEARS = _validate_yyyymm_range(ARGS.years)
        except ValueError as err:
            LOGGER.critical(str(err))
            sys.exit(2)
    else:
        LOGGER.critical('Invalid argument(s) for range of months to process')
        sys.exit(2)

    index_tables(YEARS, DBSETTING, LOGGER)
