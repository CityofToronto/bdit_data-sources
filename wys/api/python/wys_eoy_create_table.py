import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import psycopg2

trigger_sql_preamble='''CREATE OR REPLACE FUNCTION wys.insert_trigger()
RETURNS trigger
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
BEGIN
'''
trigger_sql_logic='''IF (new.datetime_bin >= '{year}-01-01' AND new.datetime_bin < '{year}-01-01'::DATE + INTERVAL '1 year') THEN 
        INSERT INTO wys.raw_data_{year} VALUES (NEW.api_id, NEW.datetime_bin, NEW.speed, NEW.count) ON CONFLICT DO NOTHING;'''
#Starts with an E because there's got to be "ELS" prepended to this 
trigger_sql_end='''E 
    RAISE EXCEPTION 'Datetime_bin out of range.  Fix the wys.insert_trigger() function!';
	END IF;
	RETURN NULL;
END;
$BODY$;
'''

def _get_year_from_dt(dt):
    next_dt = datetime.strptime(dt,  "%Y-%m-%d") + relativedelta(years=1)
    return str(next_dt.year)

def create_wys_raw_data_table(pg_hook = None, dt = None):
    '''Executes the postgresql function to create bluetooth tables for the given year'''

    logger = logging.getLogger('create_wys_raw_data_table')
    year = _get_year_from_dt(dt)
    try:
        with pg_hook.get_conn() as conn, conn.cursor() as cur:
            logger.info('Creating wys.raw_data table for year: %s', year)
            cur.execute('SELECT wys.create_raw_data_table(%s)', (year,))
    except psycopg2.Error as exc:
        logger.error('There was an error creating wys.raw_data table')
        logger.error(exc)
        raise Exception()

def replace_wys_raw_data_trigger(pg_hook = None, dt = None):
    '''Creates sql for the trigger to send data to the newly created tables and then executes it'''
    logger = logging.getLogger('replace_wys_raw_data_trigger')
    next_year = _get_year_from_dt(dt)
    conn = pg_hook.get_conn()
    sql = trigger_sql_preamble
    for year in range (int(next_year), 2015, -1):
        sql += trigger_sql_logic.format(year=year)+'\nELS'

    sql += trigger_sql_end
    logger.info('Created sql for wys.insert_trigger()')
    logger.debug(sql)
    try:
        with pg_hook.get_conn() as conn, conn.cursor() as cur:
            logger.info('Updating wys.insert_trigger()')
            cur.execute(sql)
    except psycopg2.Error as exc:
        logger.error('There was an error replacing the wys.insert_trigger()')
        logger.error(exc)
        raise Exception()
