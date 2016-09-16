#!/usr/bin/python3
'''Perform operations on Inrix data'''
import logging
import re
from time import sleep
from psycopg2 import connect, OperationalError

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
    
    if yyyy[0] == yyyy[1]:
        years[yyyy[0]] = range(mm[0], mm[1]+1)
    else:
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

if __name__ == "__main__":
    import argparse
    import json
    import sys

    PARSER = argparse.ArgumentParser(description='Index, partition, or aggregate Inrix traffic data in a database.')
    #Must have either a year range or pass a JSON file with years.
    YEARS_ARGUMENTS = PARSER.add_mutually_exclusive_group(required=True)
    YEARS_ARGUMENTS.add_argument("-y", "--years", nargs=2,
                                 help="Range of months (YYYYMM) to operate over"
                                 "from startdate to enddate",
                                 metavar=('YYYYMM', 'YYYYMM'))
    YEARS_ARGUMENTS.add_argument("-Y", "--yearsjson", type=json.loads,
                                 help="Written dictionary which contains years as key"
                                 "and start/end month like {'2012'=[1,12]}")
    #Possible action to call inrix_util to perform
    ACTIONS = PARSER.add_mutually_exclusive_group(required=True)
    ACTIONS.add_argument("-i", "--index", action="store_true",
                         help="Index the tables specified by the years argument")
    ACTIONS.add_argument("-p", "--partition", action="store_true",
                         help="Add Check Constraints to specified tables to complete table partitioning")
    ACTIONS.add_argument("-a", "--aggregate", action="store_true",
                         help="Aggregate raw data")
    PARSER.add_argument("-d", "--dbsetting",
                        default='default.cfg',
                        help="Filename with connection settings to the database"
                        "(default: opens %(default)s)")
    PARSER.add_argument("-t", "--tablename",
                        default='inrix.raw_data',
                        help="Base table on which to perform operation of form %(default)s")
    PARSER.add_argument("-tx", "--timecolumn",
                        default='tx',
                        help="Time column for partitioning, default: %(default)s")

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

    if ARGS.index:
        from create_index import index_tables
        index_tables(YEARS, DBSETTING, LOGGER)
    elif ARGS.partition:
        from finish_partition import partition_tables
        partition_tables(YEARS, DBSETTING, LOGGER, table=ARGS.tablename, timecol=ARGS.timecolumn)
    elif ARGS.aggregate:
        from aggregate import agg_tables
        agg_tables(YEARS, DBSETTING, LOGGER)
