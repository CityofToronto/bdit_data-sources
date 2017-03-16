'''Clips all layers in the here_gis schema to the Toronto boundary layer'''
from psycopg2 import connect, OperationalError, InterfaceError, ProgrammingError
from time import sleep
import logging

def try_connection(logger, dbset, *, autocommit=False, **kwargs):
    '''Connection retry loop'''
    while True:
        try:
            logger.info('Connecting to host:%s database: %s with user %s',
                        dbset['host'],
                        dbset['database'],
                        dbset['user'])
            con = connect(database=dbset['database'],
                          host=dbset['host'],
                          user=dbset['user'],
                          password=dbset['password'])
            if autocommit:
                #Necessary for index building
                con.autocommit = True
            cursor = con.cursor()
            logger.info('Testing Connection')
            cursor.execute('SELECT 1')
            cursor.fetchall()
        except OperationalError as oe:
            logger.error(oe)
            logger.info('Retrying connection in 2 minutes')
            sleep(120)
        else:
            break
    return con, cursor

def execute_function(func, logger, cursor, dbset, **kwargs):
    '''Execute a function within a psycopg2 connection retry loop'''
    while True:
        try:
            func(logger, cursor, **kwargs)
        except (OperationalError, InterfaceError) as oe:
            logger.error(oe)
            con, cursor = try_connection(logger, dbset, **kwargs)
        else:
            break

def clip(logger, cursor, *, schema_name, table_name, **kwargs):
    '''Clip the specified layer in PostgreSQL
    
    PostgreSQL function definition here
    https://github.com/CityofToronto/bdit_pgutils/blob/master/gis/clip_to'''
    logger.info('Clipping %s.%s', schema_name, table_name)
    try:
        cursor.execute('SELECT gis.clip_to(%(schema_name)s, %(table_name)s );', 
                       {'schema_name': schema_name, 'table_name': table_name})
    except ProgrammingError as pe:
        logger.error(pe)
    
if __name__ == "__main__":

    LOGGER = logging.getLogger(__name__)
    LOGGER.setLevel(logging.DEBUG)
    
    TIMEFORMAT = logging.Formatter('%(asctime)-15s,%(message)s',"%Y-%m-%d %H:%M:%S")
    
    TIMEHANDLER = logging.FileHandler('log/connection_test.log')
    TIMEHANDLER.setLevel(logging.INFO)
    TIMEHANDLER.setFormatter(TIMEFORMAT) 
        
    CONSOLEHANDLER = logging.StreamHandler()
    CONSOLEHANDLER.setLevel(logging.DEBUG)
    CONSOLEHANDLER.setFormatter(TIMEFORMAT)
    
    LOGGER.addHandler(TIMEHANDLER)
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
