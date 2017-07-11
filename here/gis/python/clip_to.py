'''Clips all layers in the here_gis schema to the Toronto boundary layer'''
from psycopg2 import connect, OperationalError, InterfaceError, ProgrammingError
from time import sleep
import logging

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../data_utils')))

from sql_action import SqlAction

class GisActions( SqlAction ):
    '''
    '''

    def __init__(self, logger, dbset, *args, **kwargs):
        super(IndexCreator, self).__init__(logger, dbsettings, *args, **kwargs)
        

    def clip(schema_name, table_name, **kwargs):
        '''Clip the specified layer in PostgreSQL

        PostgreSQL function definition here
        https://github.com/CityofToronto/bdit_pgutils/blob/master/gis/clip_to'''
        self.logger.info('Clipping %s.%s', schema_name, table_name)
        try:
            self.cursor.execute('SELECT gis.clip_to(%(schema_name)s, %(table_name)s );', 
                           {'schema_name': schema_name, 'table_name': table_name})
        except ProgrammingError as pe:
            self.logger.error(pe)
    
if __name__ == "__main__":

    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.DEBUG)
    
    LOGFORMAT = logging.Formatter('%(asctime)-15s,%(message)s',"%Y-%m-%d %H:%M:%S")
    
    FILEHANDLER = logging.FileHandler('log/clipping.log')
    FILEHANDLER.setLevel(logging.INFO)
    FILEHANDLER.setFormatter(LOGFORMAT) 
        
    CONSOLEHANDLER = logging.StreamHandler()
    CONSOLEHANDLER.setLevel(logging.DEBUG)
    CONSOLEHANDLER.setFormatter(LOGFORMAT)
    
    LOGGER.addHandler(FILEHANDLER)
    LOGGER.addHandler(CONSOLEHANDLER)
    
    import configparser
    CONFIG = configparser.ConfigParser()
    CONFIG.read('db.cfg')
    dbset = CONFIG['DBSETTINGS']
    
    con, cursor = try_connection(LOGGER, dbset, autocommit = True)
    
    schema_names = ['here_gis']
    
    for schema_name in schema_names:
        cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = %(schema_name)s;",
                       {'schema_name':schema_name})
        tables = cursor.fetchall()

        for table in tables:
            execute_function(clip, LOGGER, cursor, dbset, table_name = table[0], schema_name = schema_name)
