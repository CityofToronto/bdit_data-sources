import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
import psycopg2

trigger_sql_preamble='''CREATE OR REPLACE FUNCTION miovision_api.volumes_insert_trigger()
    RETURNS trigger
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
BEGIN
'''
trigger_sql_logic='''IF new.datetime_bin >= '{year}-01-01'::date AND new.datetime_bin < ('{year}-01-01'::date + '1 year'::interval) THEN 
		INSERT INTO miovision_api.volumes_{year} (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume) 
		VALUES (NEW.intersection_uid, NEW.datetime_bin, NEW.classification_uid, NEW.leg, NEW.movement_uid, NEW.volume);'''
#Starts with an E because there's got to be "ELS" prepended to this 
trigger_sql_end='''E 
    RAISE EXCEPTION 'Datetime_bin out of range.  Fix the volumes_insert_trigger() function!';
	END IF;
	RETURN NULL;
EXCEPTION
    WHEN UNIQUE_VIOLATION THEN 
        RAISE WARNING 'You are trying to insert duplicate data! \n %', 
		NEW.intersection_uid ||', '|| NEW.datetime_bin ||', '|| NEW.classification_uid ||', '|| NEW.leg ||', '|| NEW.movement_uid ||', '|| NEW.volume;
    RETURN NULL;
END;
$BODY$;
'''

def _get_year_from_dt(dt):
    next_dt = datetime.strptime(dt,  "%Y-%m-%d") + relativedelta(years=1)
    return str(next_dt.year)

def create_miovision_vol_table(pg_hook = None, dt = None):
    '''Executes the postgresql function to create bluetooth tables for the given year'''

    logger = logging.getLogger('create_miovision_vol_table')
    conn = pg_hook.get_conn()
    year = _get_year_from_dt(dt)
    try:
        with conn:
            with conn.cursor() as cur:
                logger.info('Creating miovision_api.volumes tables for year: %s', year)
                cur.execute('SELECT miovision_api.create_volume_table(%s)', (year,))
    except psycopg2.Error as exc:
        logger.error('There was an error creating miovision volume table')
        logger.error(exc)
        raise Exception()
    finally:
        conn.close()

def replace_miovision_vol_trigger(pg_hook = None, dt = None):
    '''Creates sql for the trigger to send data to the newly created tables and then executes it'''
    logger = logging.getLogger('replace_miovision_vol_trigger')
    next_year = _get_year_from_dt(dt)
    conn = pg_hook.get_conn()
    sql = trigger_sql_preamble
    for year in range (int(next_year), 2017, -1):
        sql += trigger_sql_logic.format(year=year)+'\nELS'

    sql += trigger_sql_end
    logger.info('Created sql for miovision_api.volumes_insert_trigger()')
    logger.debug(sql)
    try:
        with conn:
            with conn.cursor() as cur:
                logger.info('Updating miovision_api.volumes_insert_trigger()')
                cur.execute(sql)
    except psycopg2.Error as exc:
        logger.error('There was an error Updating miovision_api.volumes_insert_trigger()')
        logger.error(exc)
        raise Exception()
    finally:
        conn.close()            
