import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import psycopg2

def _get_year_from_dt(dt):
    next_dt = datetime.strptime(dt,  "%Y-%m-%d") + relativedelta(years=1)
    return str(next_dt.year)

def create_here_ta_tables(pg_hook = None, dt = None):
    '''Executes the postgresql function to create here tables for the given 
    year'''

    logger = logging.getLogger('create_here_ta_tables')
    conn = pg_hook.get_conn()
    year = _get_year_from_dt(dt)
    try:
        with conn:
            with conn.cursor() as cur:
                logger.info('Creating HERE tables for year: %s', year)
                cur.execute('SELECT here.create_yearly_tables(%s)', (year,))
    except psycopg2.Error as exc:
        logger.exception('There was an error creating HERE tables')
        logger.exception(exc)
        raise Exception()
    finally:
        conn.close()

