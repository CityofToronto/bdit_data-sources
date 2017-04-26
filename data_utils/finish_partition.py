#!/usr/bin/python3
'''Finish partitioning tables in the Inrix database.'''
from psycopg2.extensions import AsIs
from sql_action import SqlAction

class TablePartitioner( SqlAction ):
    '''Holds setting for creating multiple indexes on multiple DB tables
    
    '''
    

    def __init__(self, logger, dbsettings, *args, schemaname=None,
                 table=None, timecol=None, **kwargs):
        super(TablePartitioner, self).__init__(logger, dbsettings, *args,
                                               autocommit=True, **kwargs)
        
        self._test_schema(schemaname)
        self.schema = schemaname
        self.table = table
        self.timecol = timecol
            
    def _test_schema(self, schema):
        '''Test existence of schema'''
        self.cur.execute("SELECT schema_name FROM information_schema.schemata "
                         "WHERE schema_name =%(schema)s", {'schema':schema})
        if self.cur.rowcount == 0:
            raise ValueError('Schema name --schemaname %s does not exist', schema)
        return self

    def _partition_table(self, *, table, startdate, **kwargs):
        '''Add check constraints on the partitioned table ending with yyyymm.'''
        
        self.logger.info('Adding check constraints on table %s.%s', self.schema, table)
        self.cur.execute("ALTER TABLE %(schemaname)s.%(tablename)s "
                         "ADD CHECK (%(timecol)s >= DATE %(startdate)s "
                         "AND %(timecol)s < DATE %(startdate)s + INTERVAL '1 month')"
                         , {'schemaname':AsIs(self.schema),
                            'tablename':AsIs(table),
                            'startdate':startdate,
                            'timecol':AsIs(self.timecol)})
        return self

    def run(self, year, month, *args, **kwargs):
        '''Add check constraints on the partitioned table ending with yyyymm.'''
        yyyymm = self.get_yyyymm(year, month)

        self.execute_function(self._partition_table, 
                              table=self.table+str(yyyymm), 
                              startdate=self.get_yyyymmdd(year, month), 
                              autocommit=True)
        return self


if __name__ == "__main__":
    from utils import get_yyyymm, try_connection, execute_function
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
    con, cursor = try_connection(LOGGER, dbset, autocommit=True)
    
    for year in YEARS:
        for month in YEARS[year]:
            yyyymm = get_yyyymm(year, month)
            execute_function(partition_table, LOGGER, cursor, tablename='inrix.raw_data')
    
