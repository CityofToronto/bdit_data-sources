import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import psycopg2

trigger_sql_preamble='''CREATE OR REPLACE FUNCTION bluetooth.observations_insert_trigger()
RETURNS trigger
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
'''
trigger_sql_logic='''IF (NEW.measured_timestamp > DATE '{year}-{month}-01' AND NEW.measured_timestamp <= DATE '{year}-{month}-01' + INTERVAL '1 month') THEN
INSERT INTO bluetooth.observations_{year}{month} VALUES (NEW.*) ON CONFLICT DO NOTHING;'''
#Starts with an E because there's got to be "ELS" prepended to this 
trigger_sql_end='''E
    RAISE EXCEPTION 'Date out of range.';
END IF;
RETURN NULL;
END;
$BODY$;
'''

def _get_year_from_dt(dt):
    next_dt = datetime.strptime(dt, "%Y-%m-%d") + relativedelta(years=1)
    return str(next_dt.year)

def create_bt_obs_tables(pg_hook = None, dt = None):
    '''Executes the postgresql function to create bluetooth tables for the given year'''

    logger = logging.getLogger('create_bt_obs_tables')
    conn = pg_hook.get_conn()
    year = _get_year_from_dt(dt)
    try:
        with conn:
            with conn.cursor() as cur:
                logger.info('Creating bluetooth.observations tables for year: %s', year)
                cur.execute('SELECT bluetooth.create_obs_tables(%s)', (year,))
    except psycopg2.Error as exc:
        logger.exception('There was an error creating bluetooth obs tables')
        logger.exception(exc)
        raise Exception()
    finally:
        conn.close()

def replace_bt_trigger(pg_hook = None, yr = None):
    '''Creates sql for the trigger to send data to the newly created tables and then executes it'''
    logger = logging.getLogger('create_bt_sql_tables')
    conn = pg_hook.get_conn()
    sql = trigger_sql_preamble
    for year in range (int(yr), 2013, -1):
        for month in range(12, 0, -1):
            if month < 10:
                month= '0'+str(month)
            sql += trigger_sql_logic.format(year=year, month=month)+'\nELS'

    sql += trigger_sql_end
    logger.info('Created sql for bluetooth.observations_insert_trigger()')
    logger.debug(sql)
    try:
        with conn:
            with conn.cursor() as cur:
                logger.info('Updating bluetooth.observations_insert_trigger()')
                cur.execute(sql)
    except psycopg2.Error as exc:
        logger.exception('There was an error Updating bluetooth.observations_insert_trigger()')
        logger.exception(exc)
        raise Exception()
    finally:
        conn.close()            
