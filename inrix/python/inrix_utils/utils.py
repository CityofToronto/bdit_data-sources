from psycopg2 import connect, OperationalError

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

def try_connection(logger, dbset, **kwargs):
    '''Connection retry loop'''
    while True:
        try:
            logger.info('Connecting to host:%s database: %s with user %s',
                        dbset['database'],
                        dbset['host'],
                        dbset['user'])
            con = connect(database=dbset['database'],
                          host=dbset['host'],
                          user=dbset['user'],
                          password=dbset['password'])
            if kwargs.get('autocommit', False):
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
