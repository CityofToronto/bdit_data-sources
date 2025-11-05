import os
import logging
import pandas as pd
from numpy import nan
from psycopg2 import sql, Error
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException, AirflowSkipException

SQL_DIR = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'sql')

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def check_dst(start_date):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    tz_1 = start.astimezone().tzname()
    tz_2 = (start + timedelta(days = 1)).astimezone().tzname()
    if tz_1 == 'EDT' and tz_2 == 'EST':
        LOGGER.info(f"EDT -> EST time change occured today.")
    else:
        LOGGER.info(f"Normal day.")
    return tz_1 == 'EDT' and tz_2 == 'EST'

def fetch_and_insert_raw_tt_data(
    start_date,
    select_conn = PostgresHook('itsc_postgres'),
    insert_conn = PostgresHook('events_bot')
):
    '''Fetches data from ITS Central, processes and inserts into RDS.
    
    - Fetches data from ITS Central `traveltimepathrawdata` table.  
    - Inserts into RDS `bluetooth.itsc_tt_raw` table.
    '''
    
    # Pull raw data from Postgres database
    if not check_dst(start_date):
        select_fpath = os.path.join(SQL_DIR, 'select-itsc-tt_raw.sql')
    else:
        select_fpath = os.path.join(SQL_DIR, 'select-itsc-tt_raw_dst_safe.sql')
    
    with open(select_fpath, 'r', encoding="utf-8") as file:
        select_query = sql.SQL(file.read()).format(
            start = sql.Literal(start_date + " 00:00:00 Canada/Eastern")
        )
    try:
        with select_conn.get_conn() as con, con.cursor() as cur:
            LOGGER.info("Fetching TT data.")
            cur.execute(select_query)
            data = cur.fetchall()
            df = pd.DataFrame(data)
            df.columns=[x.name for x in cur.description]
    except Error as exc:
        LOGGER.critical("Error fetching TT data.")
        LOGGER.critical(exc)
        raise Exception() from exc
    
    #transform values for inserting
    df = df.replace({pd.NaT: None, nan: None, '': None})
    df = [tuple(x) for x in df.to_numpy()]
    
    insert_fpath = os.path.join(SQL_DIR, 'insert-tt_raw.sql')
    with open(insert_fpath, 'r', encoding="utf-8") as file:
        insert_query = sql.SQL(file.read())
        
    with insert_conn.get_conn() as con, con.cursor() as cur:
        execute_values(cur, insert_query, df, page_size = 1000)

def fetch_and_insert_raw_tt_pathdata(
    start_date,
    select_conn = PostgresHook('itsc_postgres'),
    insert_conn = PostgresHook('events_bot')
):
    '''Fetches data from ITS Central, processes and inserts into RDS.
    
    - Fetches data from ITS Central `traveltimepathdata` table.  
    - Inserts into RDS `bluetooth.itsc_tt_raw_pathdata` table.
    '''
    # Pull raw data from Postgres database
    if not check_dst(start_date):
        select_fpath = os.path.join(SQL_DIR, 'select-itsc-tt_raw_pathdata.sql')
    else:
        select_fpath = os.path.join(SQL_DIR, 'select-itsc-tt_raw_pathdata_dst_safe.sql')
    with open(select_fpath, 'r', encoding="utf-8") as file:
        select_query = sql.SQL(file.read()).format(
            start = sql.Literal(start_date + " 00:00:00 Canada/Eastern")
        )
    try:
        with select_conn.get_conn() as con, con.cursor() as cur:
            LOGGER.info("Fetching TT data.")
            cur.execute(select_query)
            data = cur.fetchall()
            df = pd.DataFrame(data)
            df.columns=[x.name for x in cur.description]
    except Error as exc:
        LOGGER.critical("Error fetching TT data.")
        LOGGER.critical(exc)
        raise Exception() from exc
    
    #transform values for inserting
    df = df.replace({pd.NaT: None, nan: None, '': None})
    df = [tuple(x) for x in df.to_numpy()]
    
    insert_fpath = os.path.join(SQL_DIR, 'insert-tt_raw_pathdata.sql')
    with open(insert_fpath, 'r', encoding="utf-8") as file:
        insert_query = sql.SQL(file.read())
        
    with insert_conn.get_conn() as con, con.cursor() as cur:
        execute_values(cur, insert_query, df, page_size = 1000)

def fetch_and_insert_tt_path_data(
    start_date,
    select_conn = PostgresHook('itsc_postgres'),
    insert_conn = PostgresHook('events_bot')
):
    '''Fetches data from ITS Central, processes and inserts into RDS.
    
    - Fetches data from ITS Central `traveltimepathconfig`, `traveltimepathfeature` tables.
    - Inserts into RDS `bluetooth.itsc_tt_paths` table.
    '''
        
    select_fpath = os.path.join(SQL_DIR, 'select-itsc-tt_paths.sql')
    with open(select_fpath, 'r', encoding="utf-8") as file:
        select_query = sql.SQL(file.read()).format(
            start = sql.Literal(start_date + " 00:00:00 Canada/Eastern")
        )
    try:
        with select_conn.get_conn() as con, con.cursor() as cur:
            LOGGER.info("Fetching TT data.")
            cur.execute(select_query)
            data = cur.fetchall()
            if data == []:
                raise AirflowSkipException('No updated routes today, skipping task.')
            df = pd.DataFrame(data)
            df.columns=[x.name for x in cur.description]
    except Error as exc:
        LOGGER.critical("Error fetching TT data.")
        LOGGER.critical(exc)
        raise Exception() from exc
    
    #transform values for inserting
    df = df.replace({pd.NaT: None, nan: None, '': None})
    df = [tuple(x) for x in df.to_numpy()]
    
    insert_fpath = os.path.join(SQL_DIR, 'insert-tt_paths.sql')
    with open(insert_fpath, 'r', encoding="utf-8") as file:
        insert_query = sql.SQL(file.read())
        
    with insert_conn.get_conn() as con, con.cursor() as cur:
        execute_values(cur, insert_query, df)
