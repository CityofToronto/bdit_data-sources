#!/usr/bin/python3
'''Create indexes for each table in the Inrix database.'''
import json
import sys
from psycopg2.extensions import AsIs
from sql_action import SqlAction

class IndexCreator( SqlAction ):
    '''Holds setting for creating multiple indexes on multiple DB tables
    
    '''
    
    SQL_FUNCTIONS = {'score': "SELECT inrix.create_raw_score_idx(%(tablename)s)",
                     'tmc': "SELECT inrix.create_raw_tmc_idx(%(tablename)s)",
                     'timestamp': "SELECT inrix.create_raw_tx_idx(%(tablename)s)"}
    
    def __init__(self, logger, dbsettings, *args, indexes=None, schemaname=None, table='raw_data', **kwargs):
        super(IndexCreator, self).__init__(logger, dbsettings, *args, autocommit=True, **kwargs)
        
        try:
            with open("index_functions.json") as f:
                self.SQL_FUNCTIONS = json.load(f)
        except IOError as error: 
            self.logger.warning('Problem reading index_functions.json, using defaults')
        except json.JSONDecodeError as error:
            self.logger.critical('JSON error in index_functions.json')
            self.logger.critical(error)
            sys.exit(2)
        
        if indexes is None:
            indexes = ['score','tmc','timestamp']
        
        if set(indexes) <= self.SQL_FUNCTIONS.keys():
            self.indexes = indexes
        else:
            raise ValueError('Invalid set of index keys {}'.format(indexes))
        #raises ValueError
        self._test_schema(schemaname)
        self.schema = schemaname
        self.table = table
            
    def _test_schema(self, schema):
        '''Test existence of schema'''
        self.cur.execute("SELECT schema_name FROM information_schema.schemata "
                         "WHERE schema_name =%(schema)s", {'schema':schema})
        if self.cur.rowcount == 0:
            raise ValueError('Schema name --schemaname %s does not exist'.format(schema))
        return self

    def _create_index(self, *, table=None, index=None, **kwargs):
        '''Create the specified index on the specified partitioned table.'''
        self.logger.info('Creating %s index', index)
        sql = self.SQL_FUNCTIONS[index]
        self.cur.execute(sql, {'tablename':table})
        return self
    
    def _analyze_table(self, *, table, **kwargs):
        '''Analyze inrix.raw_data partitioned table with table.'''
        self.logger.info('Analyzing table %s', table)
        self.cur.execute("ANALYZE %(schemaname)s.%(tablename)s", {'schemaname':AsIs(self.schema),
                                                                 'tablename':AsIs(table)})
        return self
    
    def run(self, year, month, *args, **kwargs):
        '''Create indexes for a series of tables based on the years dictionary \
        and the dbset database connection.'''
        yyyymm = self.get_yyyymm(year, month)
        tablename = self.table+str(yyyymm)
        
        self.logger.info('Creating indexes on table %s', tablename)

        for index in self.indexes:
            #Execute the specified "CREATE INDEX" sql function on the specified table
            self.execute_function(self._create_index, table=tablename, index=index, autocommit=True)

        self.execute_function(self._analyze_table, table=tablename, autocommit=True)
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
    from dbsettings import dbsetting
    indexor = IndexCreator(LOGGER, dbsettings)
    for year in YEARS:
        for month in YEARS[year]:
            indexor.run(year, month)
