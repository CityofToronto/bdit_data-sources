#!/usr/bin/python3
'''Create indexes for each table in the Inrix database.'''
import logging
import re
from time import sleep
from psycopg2 import connect, OperationalError

def get_yyyymm(yyyy, mm):
    '''Combine integer yyyy and mm into a string yyyymm.'''
    if mm < 10:
        return str(yyyy)+'0'+str(mm)
    else:
        return str(yyyy)+str(mm)

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

def _validate_yyyymm_range(yyyymmrange):
    '''Validate the two yyyymm command line arguments provided

    Args:
        yyyymmrange: List containing a start and end year-month in yyyymm format

    Returns:
        A dictionary with the processed range like {'yyyy':range(mm1,mm2+1)}

    Raises:
        ValueError: If the values entered are incorrect
    '''

    if len(yyyymmrange) != 2:
        raise ValueError('{yyyymmrange} should contain two YYYYMM arguments'
                         .format(yyyymmrange=yyyymmrange))

    regex_yyyymm = re.compile(r'20\d\d(0[1-9]|1[0-2])')
    yyyy, mm = [], []
    years = {}

    for yyyymm in yyyymmrange:
        if regex_yyyymm.fullmatch(yyyymm):
            yyyy.append(int(yyyymm[:4]))
            mm.append(int(yyyymm[-2:]))
        else:
            raise ValueError('{yyyymm} is not a valid year-month value of format YYYYMM'
                             .format(yyyymm=yyyymm))

    if yyyy[0] > yyyy[1] or (yyyy[0] == yyyy[1] and mm[0] > mm[1]):
        raise ValueError('Start date {yyyymm1} after end date {yyyymm2}'
                         .format(yyyymm1=yyyymmrange[0], yyyymm2=yyyymmrange[1]))

    for year in range(yyyy[0], yyyy[1]+1):
        if year == yyyy[0]:
            years[year] = range(mm[0], 13)
        elif year == yyyy[1]:
            years[year] = range(1, mm[1]+1)
        else:
            years[year] = range(1, 13)

    return years

def _validate_yearsjson(yearsjson):
    '''Validate the two yyyymm command line arguments provided

    Args:
        yearsjson: dictionary containing months to be processed in the format {'yyyy':[mm1,mm2]}

    Returns:
        A dictionary with the processed range like {'yyyy':range(mm1,mm2+1)}

    Raises:
        ValueError: If the values entered are incorrect
    '''
    regex_yyyy = re.compile(r'20\d\d')
    regex_mm = re.compile(r'([1-9]|0[1-9]|1[0-2])')
    years = {}

    for year in yearsjson:
        if not regex_yyyy.fullmatch(str(year)):
            raise ValueError('Year {year} is invalid input'.format(year=year))
        elif not regex_mm.fullmatch(str(yearsjson[year][0])):
            raise ValueError('For year {year}, month {mm} is not a valid month'
                             .format(year=year, mm=yearsjson[year][0]))
        elif not regex_mm.fullmatch(str(yearsjson[year][1])):
            raise ValueError('For year {year}, month {mm} is not a valid month'
                             .format(year=year, mm=yearsjson[year][1]))
        elif yearsjson[year][0] > yearsjson[year][1]:
            raise ValueError('For year {year}, first month {mm1} comes after '
                             'second month {mm2}'.format(year=year,
                                                         mm1=yearsjson[year][0],
                                                         mm2=yearsjson[year][1]))

        years[year] = range(yearsjson[year][0], yearsjson[year][1]+1)

    return years

def index_tables(years, dbset, logger):
    '''Create indexes for a series of tables based on the years dictionary \
    and the dbset database connection.'''

    con, cursor = _try_connection(logger, dbset)
    #Necessary for index building
    con.autocommit = True

    for year in years:
        for month in years[year]:
            yyyymm = get_yyyymm(year, month)

            table = 'inrix.raw_data'+yyyymm
            logger.info('Creating indexes on table %s', table)

            #Execution retry loop score
            while True:
                try:
                    _score_index_table(table, logger, cursor)
                except OperationalError as oe:
                    logger.error(oe)
                    con, cursor = _try_connection(logger, dbset)
                else:
                    break
            #Execution retry loop tmc
            while True:
                try:
                    _tmc_index_table(table, logger, cursor)
                except OperationalError as oe:
                    logger.error(oe)
                    con, cursor = _try_connection(logger, dbset)
                else:
                    break
            #Execution retry loop tx
            while True:
                try:
                    _tx_index_table(table, logger, cursor)
                except OperationalError as oe:
                    logger.error(oe)
                    con, cursor = _try_connection(logger, dbset)
                else:
                    break

    con.close()
    logger.info('Processing complete, connection to %s database %s closed',
                dbset['host'],
                dbset['database'])

    con.close()

    def _try_connection(logger, dbset):
        '''Connection retry loop'''
        while True:
            try:
                logger.info('Connecting to host:%s database: %s with user %s',
                            dbset['database'],
                            dbset['host'],
                            dbset['user'])
                con = connect(database=dbset['database'],
                              host=dbset['host'],
                              user=dbset['user'],
                              password=dbset['password'])
                cursor = con.cursor()
                logger.info('Testing Connection')
                cursor.execute('SELECT 1')
            except OperationalError as oe:
                logger.error(oe)
                logger.info('Retrying connection in 2 minutes')
                sleep(120)
            else:
                break
        return con, cursor

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
