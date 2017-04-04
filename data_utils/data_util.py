#!/usr/bin/python3
'''Perform operations on large data in postgresql'''
import argparse
import json
import sys
import logging
import re
from itertools import chain
from utils import get_yyyymm, get_yyyymmdd, try_connection, execute_function

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

def _validate_multiple_yyyymm_range(years_list):
    '''Takes a list of pairs of yearmonth strings like ['YYYYMM','YYYYMM'] and returns
    a dictionary of years[YYYY] = range(month1, month2 + 1)'''
    years = {}
    if len(years_list) == 1:
        years = _validate_yyyymm_range(years_list[0])
    else:
        for yearrange in years_list:
            years_to_add = _validate_yyyymm_range(yearrange)
            for year_to_add in years_to_add:
                if year_to_add not in years:
                    years[year_to_add] = years_to_add[year_to_add]
                else:
                    years[year_to_add] = set.union(set(years_to_add[year_to_add]),
                                                   set(years[year_to_add]))
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

def parse_args(args, prog = None, usage = None):
    '''Parser for the command line arguments'''
    PARSER = argparse.ArgumentParser(description='Index, partition, or aggregate '
                                                 'traffic data in a database.',
                                     prog=prog, usage=usage)
    #Possible action to call inrix_util to perform
    ACTIONS = PARSER.add_mutually_exclusive_group(required=True)
    ACTIONS.add_argument("-i", "--index", action="store_true",
                         help="Index the tables specified by the years argument")
    ACTIONS.add_argument("-p", "--partition", action="store_true",
                         help="Add Check Constraints to specified tables"
                              "to complete table partitioning")
    ACTIONS.add_argument("-a", "--aggregate", action="store_true",
                         help="Aggregate raw data")
    ACTIONS.add_argument("-m", "--movedata", action="store_true",
                         help="Remove data from TMCs outside Toronto")
    #Must have either a year range or pass a JSON file with years.
    YEARS_ARGUMENTS = PARSER.add_mutually_exclusive_group(required=True)
    YEARS_ARGUMENTS.add_argument("-y", "--years", nargs=2, action='append',
                                 help="Range of months (YYYYMM) to operate over"
                                 "from startdate to enddate",
                                 metavar=('YYYYMM', 'YYYYMM'))
    YEARS_ARGUMENTS.add_argument("-Y", "--yearsjson", type=json.loads,
                                 help="Written dictionary which contains years as key"
                                 "and start/end month like {'2012'=[1,12]}")
    PARSER.add_argument("-d", "--dbsetting",
                        default='default.cfg',
                        help="Filename with connection settings to the database "
                        "(default: opens %(default)s)")
    PARSER.add_argument("-t", "--tablename",
                        default='raw_data',
                        help="Base table on which to perform operation, like %(default)s")
    PARSER.add_argument("-s", "--schemaname",
                        default='inrix',
                        help="Base schema on which to perform operation, like %(default)s")
    PARSER.add_argument("-tx", "--timecolumn",
                        default='tx',
                        help="Time column for partitioning, default: %(default)s")
    PARSER.add_argument("--alldata", action="store_true",
                        help="For aggregating, specify using all rows, regardless of score")
    PARSER.add_argument("--tmctable", default='gis.inrix_tmc_tor',
                        help="Specify subset of tmcs to use, default: %(default)s")
    PARSER.add_argument("--idx", action='append', 
                        help="Index functions to call, parameters are keys "
                        "for index_functions.json") 
    return PARSER.parse_args(args)

if __name__ == "__main__":

    ARGS = parse_args(sys.argv[1:])

    #Configure logging
    FORMAT = '%(asctime)s %(name)-2s %(levelname)-2s %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    LOGGER = logging.getLogger(__name__)

    import configparser
    CONFIG = configparser.ConfigParser()
    CONFIG.read(ARGS.dbsetting)
    dbset = CONFIG['DBSETTINGS']

    if ARGS.yearsjson:
        try:
            YEARS = _validate_yearsjson(ARGS.yearsjson)
        except ValueError as err:
            LOGGER.critical(str(err))
            sys.exit(2)
    elif ARGS.years:
        try:
            YEARS = _validate_multiple_yyyymm_range(ARGS.years)
        except ValueError as err:
            LOGGER.critical(str(err))
            sys.exit(2)
    else:
        LOGGER.critical('Invalid argument(s) for range of months to process')
        sys.exit(2)

    function = None
    kwargs = {}

    if ARGS.index:
        from create_index import IndexCreator
        #Assume indexing on all columns, unless a specific column is specified
        #If a specific column is specified
        if ARGS.idx is None:
            LOGGER.critical('--idx flag needs at least one key')
            sys.exit(2)

        try:
            indexor = IndexCreator(LOGGER, dbset, indexes=ARGS.idx, schemaname=ARGS.schemaname)
        except ValueError as err:
            LOGGER.critical(err)
            sys.exit(2)
    elif ARGS.partition:
        from finish_partition import partition_table
        kwargs['tablename'] = ARGS.schemaname + '.' + ARGS.tablename
        function=partition_table
    elif ARGS.aggregate:
        if ARGS.tmctable:
            TMCTABLE = ARGS.tmctable.split(".")
            if len(TMCTABLE) != 2:
                LOGGER.fatal('tmc table must be of form "schema.table"')
                sys.exit(2)
            kwargs['tmcschema'] = TMCTABLE[0]
            kwargs['tmctable'] = TMCTABLE[1]
        from aggregate import agg_table
        kwargs['alldata']=ARGS.alldata
        function=agg_table
    elif ARGS.movedata:
        from move_data import move_data

    #con, cursor = try_connection(LOGGER, dbset, autocommit=True)

    for year in YEARS:
        for month in YEARS[year]:
            yyyymm = get_yyyymm(year, month)
            kwargs['startdate']=get_yyyymmdd(year, month)
            if ARGS.aggregate or ARGS.partition: 
                execute_function(function, LOGGER, cursor, dbset, 
                                 autocommit=True,
                                 **kwargs, 
                                 yyyymm=yyyymm)
            elif ARGS.index: 
                indexor.run(yyyymm, table=ARGS.tablename)
            elif ARGS.movedata:
                move_data(yyyymm, LOGGER, cursor, dbset, **kwargs)
            
    #con.close()
    LOGGER.info('Processing complete, connection to %s database %s closed',
                dbset['host'],
                dbset['database'])
