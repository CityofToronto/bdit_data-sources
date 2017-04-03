#!/usr/bin/python3
'''Create indexes for each table in the Inrix database.'''
import logging
from psycopg2.extensions import AsIs
from sql_action import SqlAction

class IndexCreator( SqlAction ):
    '''Holds setting for creating multiple indexes on multiple DB tables
    
    '''
    
    SQL_FUNCTIONS = {'score': "SELECT inrix.create_raw_score_idx(%(tablename)s)",
                     'tmc': "SELECT inrix.create_raw_tmc_idx(%(tablename)s)",
                     'timestamp': "SELECT inrix.create_raw_tx_idx(%(tablename)s)"}
    
    def __init__(self, logger, dbsettings, *args, indexes=['score','tmc','timestamp'], **kwargs)
        super(IndexCreator, self).__init__(logger, dbsettings, *args, **kwargs)
        
        try:
            with open("index_functions.json", r) as f:
                self.SQL_FUNCTIONS = json.load(f)
        except IOError as error: 
            pass
        
        if set(indexes) <= self.SQL_FUNCTION.keys():
            self.indexes = indexes
        else:
            raise ValueError('Invalid set of index keys %s', indexes)

    def _create_index(self, *, table, index, **kwargs):
        '''Create tx index on the inrix.raw_data partitioned table with table.'''
        self.logger.info('Creating %s index', index)
        sql = self.SQL_FUNCTIONS[index]
        self.cur.execute(sql, {'tablename':table})

    def _analyze_table(self, *, table, **kwargs):
        '''Analyze inrix.raw_data partitioned table with table.'''
        self.logger.info('Analyzing table %s', table)
        self.cur.execute("ANALYZE inrix.%(tablename)s", {'tablename':AsIs(table)})

    def run(yyyymm, *, table='raw_data', **kwargs):
        '''Create indexes for a series of tables based on the years dictionary \
        and the dbset database connection.'''

        tablename = table+yyyymm
        logger.info('Creating indexes on table %s', tablename)

        for index in self.indexes:
            #Execute the specified "CREATE INDEX" sql function on the specified table
            self.execute_function(self._create_index, table=tablename, index=index, autocommit=True)

        self.execute_function(self._analyze_table, table=tablename, autocommit=True)

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
    indexor = IndexCreator(LOGGER, dbsettings)
    indexor.index_tables(YEARS, LOGGER, cursor, dbsettings)
