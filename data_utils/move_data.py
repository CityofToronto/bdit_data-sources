#!/usr/bin/python3
'''Move Inrix Data which is not in Toronto City Boundaries out of main Inrix tables'''
import logging
from utils import get_yyyymm, get_yyyymmdd, try_connection, execute_function
from finish_partition import partition_table
def _move_data_table(logger, cursor, *, yyyymm, **kwargs):
    '''Move outside data from TMCs outside Toronto to a new schema'''
    logger.info('Moving data in table inrix.raw_data%s', yyyymm)
    cursor.execute("SELECT inrix.movedata(%(yyyymm)s)", {'yyyymm':yyyymm})
    
def _remove_outside_data(logger, cursor, *, yyyymm, **kwargs):
    '''Then delete it from inrix.raw_data'''
    logger.info('Removing outside data from table inrix.raw_data%s', yyyymm)
    cursor.execute("SELECT inrix.removeoutsidedata(%(yyyymm)s)", {'yyyymm':yyyymm})

def move_data(yyyymm, logger, cursor, dbset, *, startdate, **kwargs):
    '''Move outside data to a new schema and then delete it from inrix.raw_data \
    using years dictionary and the dbset database connection.'''

    execute_function(_move_data_table, logger, cursor, dbset, yyyymm=yyyymm, autocommit=True)
    execute_function(_remove_outside_data, logger, cursor, dbset, yyyymm=yyyymm, autocommit=True)
    execute_function(partition_table, logger, cursor, dbset,
                     autocommit=True,
                     timecol='tx',
                     tablename = 'inrix.raw_data',
                     yyyymm = yyyymm,
                     startdate = startdate)

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
