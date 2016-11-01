from psycopg2 import connect, OperationalError, InterfaceError
from time import sleep

def get_yyyymm(yyyy, mm):
    '''Combine integer yyyy and mm into a string yyyymm.'''
    if mm < 10:
        return str(yyyy)+'0'+str(mm)
    else:
        return str(yyyy)+str(mm)
    
def get_yyyymmdd(yyyy, mm, **kwargs):
    '''Combine integer yyyy and mm into a string date yyyy-mm-dd.'''
    
    if 'dd' not in kwargs:
        dd = '01'    
    elif kwargs['dd'] >= 10:
        dd = str(kwargs['dd'])
    elif kwargs['dd'] < 10:
        dd = '0'+str(kwargs['dd'])

    if mm < 10:
        return str(yyyy)+'-0'+str(mm)+'-01'
    else:
        return str(yyyy)+'-'+str(mm)+'-01'

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