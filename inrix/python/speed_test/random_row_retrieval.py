#!/usr/bin/python3
'''Create indexes for each table in the Inrix database.'''
import logging
import time
import random
import radar
from psycopg2 import connect, OperationalError, InterfaceError
from psycopg2.extensions import AsIs

def _get_tmcs(cur, tablename):
    cur.execute("SELECT DISTINCT tmc FROM %(tablename)s", {'tablename':AsIs(tablename)})
    return cur.fetchall()

def _get_timerange(cur, tablename):
    cur.execute("SELECT MIN(tx), MAX(tx) FROM %(tablename)s", {'tablename':AsIs(tablename)})
    return cur.fetchone()

def _retrieve_raw_data(cur, tablename, tx, tmc):
    cur.execute("SELECT tx, tmc, score, speed FROM %(tablename)s "
                "WHERE tmc = %(tmc)s "
                "AND score = 30 "
                "AND tx >= %(tx)s "
                "AND tx < %(tx)s + INTERVAL '30 minutes';",
                {'tablename':AsIs(tablename), 'tx':tx, 'tmc':tmc})

def speed_test(tablename, dbset, logger, timelogger, numtests):
    '''Run a number (numtests) of random row retrievals on table tablename

    Args:
        tablename: the name of the table to access
        dbset: the database connection settings
        logger: logger object for information
        timelogger: logger object to store query timing information
        numtests: the number of queries to perform
    Returns:
        None
    '''
    logger.info('%s Connecting to host:%s database: %s with user %s',
                time.time(),
                dbset['database'],
                dbset['host'],
                dbset['user'])
    con = connect(database=dbset['database'],
                  host=dbset['host'],
                  user=dbset['user'],
                  password=dbset['password'])
    cur = con.cursor()

    logger.info('%s Retrieving distinct tmcs from table %s', time.time(), tablename)
    tmcs = _get_tmcs(cur, tablename)
    logger.info('%s Retrieving min, max dates from table %s', time.time(), tablename)
    timerange = _get_timerange(cur, tablename)
    logger.info('%s Retrieving tmcs and dates retrieved from table %s', time.time(), tablename)

    for i in range(numtests):
        tmc = random.choice(tmcs)[0]
        tx = radar.random_datetime(start=timerange[0], stop=timerange[1])

        time1 = time.time()
        while True:
            try:
                _retrieve_raw_data(cur, tablename, tx, tmc)
            except OperationalError as oe:
                logger.error(oe)

                try:
                    logger.info('Testing Connection')
                    cur.execute('SELECT 1')
                except (OperationalError, InterfaceError) as oe:
                    logger.error(oe)
                    logger.info('Retrying connection in 2 minutes')
                    time.sleep(120)
                    con = connect(database=dbset['database'],
                                  host=dbset['host'],
                                  user=dbset['user'],
                                  password=dbset['password'])
                    cur = con.cursor()
            else:
                break

        timelogger.info('%s, %s, %s, %s, %s',
                        time.time(),
                        'random_row_retrieval_index_score30',
                        tablename,
                        i,
                        time.time() - time1)

    logger.info('%s Testing complete. Connection to %s database %s closed',
                time.time(),
                dbset['host'],
                dbset['database'])


if __name__ == "__main__":
    import argparse
    PARSER = argparse.ArgumentParser(description='Run random retrieval tests on raw Inrix Data.')
    PARSER.add_argument("-n", "--number",
                        type=int,
                        default=1,
                        help="Number of tests to perform (default: %(default)s)")
    PARSER.add_argument("-d", "--dbsetting",
                        default='default.cfg',
                        help="Filename with connection settings to the database"
                        "(default: opens %(default)s)")
    PARSER.add_argument("-t", "--tablename",
                        default='inrix.raw_data201604',
                        help="Table on which to retrieve data (default: %(default)s)")
    ARGS = PARSER.parse_args()

    #Configure logging
    TIMEFORMAT = logging.Formatter('%(message)s')
    TIMEHANDLER = logging.FileHandler('log/time.log')
    TIMEHANDLER.setFormatter(TIMEFORMAT)
    TIMELOGGER = logging.getLogger('timelog')
    TIMELOGGER.addHandler(TIMEHANDLER)
    logging.basicConfig(level=logging.INFO)
    LOGGERHANDLER = logging.FileHandler('log/test.log')
    LOGGERHANDLER.setFormatter(logging.Formatter('%(asctime)-15s %(message)s'))
    LOGGER = logging.getLogger(__name__)
    LOGGER.addHandler(LOGGERHANDLER)


    import configparser
    CONFIG = configparser.ConfigParser()
    CONFIG.read(ARGS.dbsetting)
    DBSETTING = CONFIG['DBSETTINGS']

    TABLENAME = ARGS.tablename
    NUMTEST = ARGS.number
    speed_test(TABLENAME, DBSETTING, LOGGER, TIMELOGGER, NUMTEST)
