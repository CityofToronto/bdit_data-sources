import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import psycopg2

def _get_year_from_dt(dt):
    next_dt = datetime.strptime(dt,  "%Y-%m-%d") + relativedelta(years=1)
    return next_dt.year

def create_here_ta_tables(pg_hook = None, dt = None):
    '''Executes the postgresql function to create here tables for the given 
    year'''

    logger = logging.getLogger('create_here_ta_tables')
    conn = pg_hook.get_conn()
    year = _get_year_from_dt(dt)
    try:
        with conn.cursor() as cur:
            logger.info('Creating HERE tables for year: %s', dt)
            cur.execute('SELECT here.create_tables(%s)', year)
    except psycopg2.Error as exc:
        logger.exception('There was an error creating HERE tables')
        logger.exception(exc)
        raise Exception(exc)

def create_sql_for_trigger(dt):
    '''Creates sql for the trigger to send data to the newly created tables'''
    logger = logging.getLogger('create_here_ta_tables')
    year = _get_year_from_dt(dt)

    insert_statement = '''IF (NEW.tx >= DATE '{year}-{month}-01' AND NEW.tx < DATE '{year}-{month}-01' +INTERVAL '1 month') THEN
    INSERT INTO here.ta_{year}{month} VALUES (NEW.*)ON CONFLICT DO NOTHING;'''
    sql = insert_statement.format(year=year, month=12)
    for month in range(11, 0, -1):
        if month < 10:
            month= '0'+str(month)
        sql += '\nELS'+insert_statement.format(year=year, month=month)

    return sql

