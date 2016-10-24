#!/usr/bin/python3
'''Move Inrix Data which is not in Toronto City Boundaries out of main Inrix tables'''
import logging
from psycopg2 import connect, OperationalError, InterfaceError
from utils import get_yyyymm, try_connection

def _move_data_table(yyyymm, logger, cursor, con, **kwargs):
    '''Move outside data from TMCs outside Toronto to a new schema'''

    logger.info('Moving data in table inrix.raw_data%s', yyyymm)
    cursor.execute("SELECT inrix.movedata(%(yyyymm)s)", {'yyyymm':yyyymm})
    
def _remove_outside_data(yyyymm, logger, cursor, con, **kwargs):
    '''Then delete it from inrix.raw_data'''
    logger.info('Removing outside data from table inrix.raw_data%s', yyyymm)
    cursor.execute("SELECT inrix.removeoutsidedata(%(yyyymm)s)", {'yyyymm':yyyymm})


def move_data(years, dbset, logger, **kwargs):
    '''Move outside data to a new schema and then delete it from inrix.raw_data \
    using years dictionary and the dbset database connection.'''

    con, cursor = try_connection(logger, dbset, autocommit=True)

    for year in years:
        for month in years[year]:
            yyyymm = get_yyyymm(year, month)
            #Execution retry loop
            while True:
                try:
                    _move_data_table(yyyymm, logger, cursor, con, **kwargs)
                except (OperationalError, InterfaceError) as oe:
                    logger.error(oe)
                    con, cursor = try_connection(logger, dbset, autocommit=True)
                else:
                    break
                    
            while True:
                try:
                    _remove_outside_data(yyyymm, logger, cursor, con, **kwargs)
                except (OperationalError, InterfaceError) as oe:
                    logger.error(oe)
                    con, cursor = try_connection(logger, dbset, autocommit=True)
                else:
                    break

    con.close()
    logger.info('Processing complete, connection to %s database %s closed',
                dbset['host'],
                dbset['database'])

if __name__ == "__main__":
    #For initial run, creating years and months of available data as a python dictionary
    YEARS = {"2012":range(7, 13),
             "2013":range(1, 13),
             "2011":range(8, 13),
             "2016":range(1, 7),
             "2014":range(1, 13),
             "2015":range(1, 13)}
    #Configure logging
    FORMAT = '%(asctime)-15s %(message)s'
    logging.basicConfig(level=logging.INFO, format=FORMAT)
    LOGGER = logging.getLogger(__name__)
    from dbsettings import dbsetting
    move_data(YEARS, dbsetting, LOGGER)
