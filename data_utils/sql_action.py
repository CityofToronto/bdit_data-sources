from psycopg2 import connect, OperationalError, InterfaceError
from time import sleep

class SqlAction( object ):
    '''Base class for holding settings to automate functions on the DB
    '''

    def __init__(self, logger, dbsettings, *args, **kwargs):
        self.logger = logger
        self.dbsettings = dbsettings
        self.con, self.cur = self.try_connection(**kwargs)
        
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

    def try_connection(self, *, autocommit=False, **kwargs):
        '''Connection retry loop'''
        while True:
            try:
                self.logger.info('Connecting to host:%s database: %s with user %s',
                            self.dbsettings['host'],
                            self.dbsettings['database'],
                            self.dbsettings['user'])
                con = connect(**self.dbsettings)
                if autocommit:
                    #Necessary for index building
                    con.autocommit = True
                cursor = con.cursor()
                self.logger.info('Testing Connection')
                cursor.execute('SELECT 1')
                cursor.fetchall()
            except OperationalError as oe:
                self.logger.error(oe)
                self.logger.info('Retrying connection in 2 minutes')
                sleep(120)
            else:
                break
        return con, cursor


    def execute_function(self, func, **kwargs):
        '''Execute a function within a psycopg2 connection retry loop'''
        while True:
            try:
                func(**kwargs)
            except (OperationalError, InterfaceError) as oe:
                self.logger.error(oe)
                self.con, self.cursor = self.try_connection(**kwargs)
            else:
                break