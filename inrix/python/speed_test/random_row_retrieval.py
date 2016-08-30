#!/usr/bin/python3
'''Create indexes for each table in the Inrix database.'''
import logging
from psycopg2 import connect, OperationalError, InterfaceError
from psycopg2.extensions import AsIs
import datetime
import time
import radar
import random

def _get_tmcs(cur, tablename):
    
    cur.execute("SELECT DISTINCT tmc FROM %(tablename)s", {'tablename':AsIs(tablename)})
    return cur.fetchall()

def _get_timerange(cur, tablename):
    cur.execute("SELECT MIN(tx), MAX(tx) FROM %(tablename)s", {'tablename':AsIs(tablename)})
    return cur.fetchone()

def _retrieve_raw_data(cur, tablename, tx, tmc):

    cur.execute("SELECT tx, tmc, score, speed FROM %(tablename)s "
                "WHERE tmc = %(tmc)s "
                "AND tx >= %(tx)s "
                "AND tx < %(tx)s + INTERVAL '30 minutes';",
                {'tablename':AsIs(tablename),'tx':tx,'tmc':tmc})
    
def speed_test(tablename, dbset, logger, numtests):

    logger.info('%s Connecting to host:%s database: %s with user %s,,',
                time.time(),
                dbset['database'],
                dbset['host'],
                dbset['user'])
    con = connect(database=dbset['database'],
                  host=dbset['host'],
                  user=dbset['user'],
                  password=dbset['password'])
    cur = con.cursor()
    
    logger.info('%s Retrieving distinct tmcs from table %s,,', time.time(), tablename)
    tmcs = _get_tmcs(cur, tablename)
    logger.info('%s Retrieving min, max dates from table %s,,', time.time(), tablename)
    timerange = _get_timerange(cur, tablename)
    logger.info('%s Retrieving tmcs and dates retrieved from table %s,,', time.time(), tablename)
    
    for i in range(numtests):
        tmc = random.choice(tmcs)[0]
        tx = radar.random_datetime(start=timerange[0],stop=timerange[1])
        
        time1 = time.time()
        while True:
            try:
                _retrieve_raw_data(cur, tablename, tx, tmc)
            except OperationalError as oe:
                logger.error(oe)

                try:
                    logger.info('Testing Connection')
                    cursor.execute('SELECT 1')
                except OperationalError as oe:
                    logger.error(oe)
                    logger.info('Retrying connection in 2 minutes')
                    sleep(120)
                    con = connect(database=dbset['database'],
                                  host=dbset['host'],
                                  user=dbset['user'],
                                  password=dbset['password'])
                    cursor = con.cursor()
            else:
                break
        
        timelogger.info('%s, %s, %s',
                    tablename, 
                    i,
                    time.time() - time1)
    
    logger.info('%s Testing complete. Connection to %s database %s closed,,',
                time.time(),
                dbset['host'],
                dbset['database'])


if __name__ == "__main__":
    
    #Configure logging
    FORMAT = '%(message)s'
    logging.basicConfig(filename='log/test.log', level=logging.INFO, format=FORMAT)
    LOGGER = logging.getLogger(__name__)
    
    from dbsettings import dbsetting
    TABLENAME = 'inrix.raw_data201604'
    NUMTEST = 1
    speed_test(TABLENAME, dbsetting, LOGGER, NUMTEST)
