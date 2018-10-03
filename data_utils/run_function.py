#!/usr/bin/python3
'''Create indexes for each table in the Inrix database.'''
import json
import sys
from psycopg2 import sql
from sql_action import SqlAction

class FunctionRunner( SqlAction ):
    '''Holds setting for running a function on multiple DB tables
    
    '''
    
    def __init__(self, logger, dbsettings, *args, function=None, schemaname=None, **kwargs):
        super(FunctionRunner, self).__init__(logger, dbsettings, *args, autocommit=True, **kwargs)
        
        self._test_schema(schemaname)
        self.schema = schemaname
        self.function = function
        self.sql_function = sql.SQL('SELECT '+schemaname+'.'+function+'(%(yyyymm)s);')
            
    def _test_schema(self, schema):
        '''Test existence of schema'''
        self.cur.execute("SELECT schema_name FROM information_schema.schemata "
                         "WHERE schema_name =%(schema)s", {'schema':schema})
        if self.cur.rowcount == 0:
            raise ValueError('Schema name --schemaname {} does not exist'.format(schema))
        return self

    def _run_function(self, *, yyyymm=None, **kwargs):
        '''Run the specified function for the specified month.'''
        self.cur.execute(self.sql_function, {'yyyymm':yyyymm})
        return self
        
    def run(self, year, month, *args, **kwargs):
        '''Run the specified sql function for a series of months based on the years dictionary \
        and the dbset database connection.'''
        yyyymm = self.get_yyyymm(year, month)
        
        self.logger.info('Running function %s.%s for %s', self.schema, self.function, yyyymm)

        #Execute the specified sql function for specified month
        self.execute_function(self._run_function, yyyymm=yyyymm, autocommit=True)

        return self
    
if __name__ == "__main__":
    import logging
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
    import configparser
    CONFIG = configparser.ConfigParser()
    CONFIG.read('db.cfg')
    dbset = CONFIG['DBSETTINGS']
    functionrunner = FunctionRunner(LOGGER, dbset)
    for year in YEARS:
        for month in YEARS[year]:
            functionrunner.run(year, month)
