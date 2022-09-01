"""
Pipeline for pulling Red Light Camera data from Open Data API in json format via
https://secure.toronto.ca/opendata/cart/red_light_cameras.json (see
https://secure.toronto.ca/opendata/cart/red_light_cameras/details.html). This
json file will be stored in the existing table 'vz_safety_programs_staging.rlc'
in the bigdata RDS (table will be truncated each time this script is called).

(Added on Aug 03 2022)
Also contains the pipeline for pulling:
- Pedestrian Head Start Signals/Leading Pedestrian Intervals (LPI)
- Accessible Pedestrian Signals (APS)
- Pedestrian Crossovers (PXO)
- General traffic signals

"""
from datetime import datetime
import os
import sys
from threading import local
import psycopg2
import requests
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from dateutil.parser import parse
from datetime import datetime

# Credentials
from airflow.hooks.postgres_hook import PostgresHook
vz_cred = PostgresHook("vz_api_bot") # name of Conn Id defined in UI
#vz_pg_uri = vz_cred.get_uri() # connection to RDS for psql via BashOperator
conn = vz_cred.get_conn() # connection to RDS for python via PythonOperator

# ------------------------------------------------------------------------------
# Slack notification
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed / Tâche échouée. LOCALHOST AIFRLOW
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        proxy='http://137.15.73.132:8080'
        )
    return failed_alert.execute(context=context)

# ------------------------------------------------------------------------------
AIRFLOW_DAGS = os.path.dirname(os.path.realpath(__file__))
AIRFLOW_ROOT = os.path.dirname(AIRFLOW_DAGS)
AIRFLOW_TASKS = os.path.join(AIRFLOW_ROOT, 'assets/rlc/airflow/tasks')

DEFAULT_ARGS = {
    'email': ['Cathy.Nangini@toronto.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'owner': 'airflow',
    'start_date': datetime(2019, 9, 16), # YYYY, MM, DD
    'task_concurrency': 1,
    'on_failure_callback': task_fail_slack_alert
}

# ------------------------------------------------------------------------------

def pull_rlc(conn):
    '''
    Connect to bigdata RDS, pull Red Light Camera json file from Open Data API,
    and overwrite existing rlc table in the vz_safety_programs_staging schema.
    '''

    local_table='vz_safety_programs_staging.rlc'
    url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/9fcff3e1-3737-43cf-b410-05acd615e27b/resource/7e4ac806-4e7a-49d3-81e1-7a14375c9025/download/Red%20Light%20Cameras%20Data.geojson"  
    return_json = requests.get(url).json()
    
    rlcs = return_json['features']
    rows = []
    
    # column names in the PG table
    col_names = ['rlc','tcs','loc','additional_info','main','side1','side2','mid_block','private_access','x','y','district','ward1','ward2','ward3','ward4','police_division_1','police_division_2','police_division_3','date_installed','longitude','latitude'] 

    # attribute names in JSON dict
    att_names = ['RLC','TCS','NAME','ADDITIONAL_INFO','MAIN','SIDE1','SIDE2','MID_BLOCK','PRIVATE_ACCESS','X','Y','DISTRICT','WARD_1','WARD_2','WARD_3','WARD_4','POLICE_DIVISION_1','POLICE_DIVISION_2','POLICE_DIVISION_3','ACTIVATION_DATE'] 

    # each "info" is all the properties of one RLC, including its coords
    for info in rlcs:
        # temporary list of properties of one RLC to be appended into the rows list
        one_rlc = []

        # dive deeper into the json objects
        properties = info['properties']
        geom = info['geometry']
        coords = geom['coordinates']

        # append the values in the same order as in the table
        for attr in att_names:
            one_rlc.append(properties[attr])
        one_rlc += coords # or just coords if it's already a list of just these two elements

        rows.append(tuple(one_rlc))
    
    # truncate and insert into the local table
    insert = """INSERT INTO vz_safety_programs_staging.rlc ({columns}) VALUES %s"""
    insert_query = sql.SQL(insert).format(
        columns = sql.SQL(',').join([
            sql.Identifier(col_names[0]),
            sql.Identifier(col_names[1]),
            sql.Identifier(col_names[2]),
            sql.Identifier(col_names[3]),
            sql.Identifier(col_names[4]),
            sql.Identifier(col_names[5]),
            sql.Identifier(col_names[6]),
            sql.Identifier(col_names[7]),
            sql.Identifier(col_names[8]),
            sql.Identifier(col_names[9]),
            sql.Identifier(col_names[10]),
            sql.Identifier(col_names[11]),
            sql.Identifier(col_names[12]),
            sql.Identifier(col_names[13]),
            sql.Identifier(col_names[14]),
            sql.Identifier(col_names[15]),
            sql.Identifier(col_names[16]),
            sql.Identifier(col_names[17]),
            sql.Identifier(col_names[18]),
            sql.Identifier(col_names[19]),
            sql.Identifier(col_names[20]),
            sql.Identifier(col_names[21])
        ]))
    
    with conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE {}".format(local_table))
            execute_values(cur, insert_query, rows)

# ------------------------------------------------------------------------------

def insert_data(conn, col_names, rows, ts_type):
    
    '''
    Insert a particular type of traffic signal into vz_safety_programs_staging.signals_cart
    after first deleting entries of that particular traffic signal. RLCs are not inserted
    through this function.
    
    Params
    conn: connection to RDS for python via PythonOperator
    col_names: column names in the PG table
    rows: rows of information about traffic signal to be inserted
    ts_type: a string that indicates the type of traffic signal
    
    '''
    
    insert = """INSERT INTO vz_safety_programs_staging.signals_cart ({columns}) VALUES %s"""
    insert_query = sql.SQL(insert).format(
        columns = sql.SQL(',').join([
            sql.Identifier(col_names[0]),
            sql.Identifier(col_names[1]),
            sql.Identifier(col_names[2]),
            sql.Identifier(col_names[3]),
            sql.Identifier(col_names[4]),
            sql.Identifier(col_names[5]),
            sql.Identifier(col_names[6]),
            sql.Identifier(col_names[7]),
            sql.Identifier(col_names[8]),
            sql.Identifier(col_names[9])
        ]))
    
    delete = """DELETE FROM vz_safety_programs_staging.signals_cart WHERE asset_type = %s"""
    delete_query = sql.SQL(delete)
    
    with conn:
        with conn.cursor() as cur:
            cur.execute(delete_query, (ts_type,))
            execute_values(cur, insert_query, rows)

# ------------------------------------------------------------------------------
# Pull APS data
def pull_aps(conn):
    
    local_table='vz_safety_programs_staging.signals_cart'
    url = "https://secure.toronto.ca/opendata/cart/traffic_signals/v3?format=json"
    return_json = requests.get(url).json()
    
    rows = []

    # column names in the PG table
    col_names = ['asset_type','px','main_street','midblock_route','side1_street','side2_street','latitude','longitude','activation_date','details']

    # attribute names in JSON dict
    att_names = ['px', 'main', 'mid_block', 'side1', 'side2', 'lat', 'long', 'aps_activation_date', 'aps_operation']

    # each "info" is all the properties of one APS, including its coords
    for obj in return_json:
        if obj['aps_signal'] == "1":

            # temporary list of properties of one APS to be appended into the rows list
            one_aps = []

            one_aps.append('Audible Pedestrian Signals') # append the asset_name as listed in EC2

            # append the values in the same order as in the table
            for attr in att_names:
                one_aps.append(obj[attr])

            rows.append(tuple(one_aps))
    
    # delete existing APS and insert into the local table
    insert_data(conn, col_names, rows, 'Audible Pedestrian Signals')

# ------------------------------------------------------------------------------
# Pull PXO data
def pull_pxo(conn):
    
    local_table='vz_safety_programs_staging.signals_cart'
    url = "https://secure.toronto.ca/opendata/cart/pedestrian_crossovers/v2?format=json"
    return_json = requests.get(url).json()
    
    rows = []

    # column names in the PG table
    col_names = ['asset_type','px','main_street','midblock_route','side1_street','side2_street','latitude','longitude','activation_date','details']

    # attribute names in JSON dict
    att_names = ['px', 'main', 'midblock', 'side1', 'side2', 'lat', 'long', 'activation_date', 'additional_info']

    # each "info" is all the properties of one RLC, including its coords
    for pxo in return_json:
        # temporary list of properties of one RLC to be appended into the rows list
        one_pxo = []

        one_pxo.append('Pedestrian Crossovers') # append the asset_name as listed in EC2

        # append the values in the same order as in the table
        for attr in att_names:
            one_pxo.append(pxo[attr])

        rows.append(tuple(one_pxo))
    
    # delete existing PXO and insert into the local table
    insert_data(conn, col_names, rows, 'Pedestrian Crossovers')

# ------------------------------------------------------------------------------
# Pull LPI data
imp_date_directions = ['lpiNorthImplementationDate', 'lpiSouthImplementationDate', 'lpiEastImplementationDate', 'lpiWestImplementationDate']

def lastest_imp_date(obj):
    """
    Returns either a string that is the latest LPI installation date of the four possible directions,
    or None if none of the four directions have a date string
    """
    
    date_strings = []
    for direction in imp_date_directions:
        if obj[direction] is not None:
            date_strings.append(obj[direction])
    
    # If the 'date_strings' list is empty
    if not date_strings:
        return None
    
    else:
        # If there is only one valid implementation date, return it
        if len(date_strings) == 1:
            return date_strings[0]
        
        # If there are more than one valid implementation dates, make comparisons and return the latest one
        else:
            
            # Convert date strings into datetime objects for comparison
            datetime_format_dates = []
            for date_string in date_strings:
                
                try:
                    datetime_date = parse(date_string, fuzzy=False)
                    datetime_format_dates.append(datetime_date)
                except ValueError:
                    pass
            
            # Case 1: none of the strings in 'date_strings' can be converted to datetime objects
            if not datetime_format_dates:
                return None
            # Case 2: only one date string can be converted into a datetime object
            elif len(datetime_format_dates) == 1:
                formatted_date = datetime_format_dates[0].strftime("%Y-%m-%d")
                return formatted_date
            # Case 3: more than one date string can be converted into datetime objects
            else:
                latest_date = datetime_format_dates[0]
                for date in datetime_format_dates[1:]:
                    if date > latest_date:
                        latest_date = date
                formatted_date = latest_date.strftime("%Y-%m-%d")
                return formatted_date

def pull_lpi(conn):
    
    local_table='vz_safety_programs_staging.signals_cart'
    url = "https://secure.toronto.ca/opendata/cart/traffic_signals/v3?format=json"
    return_json = requests.get(url).json()
    
    rows = []

    # column names in the PG table
    col_names = ['asset_type','px','main_street','midblock_route','side1_street','side2_street','latitude','longitude','details','activation_date']

    # attribute names in JSON dict
    att_names = ['px', 'main', 'mid_block', 'side1', 'side2', 'lat', 'long', 'lpiComment']

    # each "info" is all the properties of one LPI, including its coords
    for obj in return_json:
        if obj['leading_pedestrian_intervals'] == 1:

            # temporary list of properties of one LPI to be appended into the rows list
            one_lpi = []

            one_lpi.append('Leading Pedestrian Intervals') # append the asset_name as listed in EC2

            # append the values in the same order as in the table
            for attr in att_names:
                one_lpi.append(obj[attr])

            # append the latest LPI implementation date of any of the four directions
            one_lpi.append(lastest_imp_date(obj))

            rows.append(tuple(one_lpi))
    
    # delete existing LPI and insert into the local table
    insert_data(conn, col_names, rows, 'Leading Pedestrian Intervals')

# ------------------------------------------------------------------------------
''' 
Pull Traffic Signal Data (All of them) into signals_cart, in case a traffic signal was not pulled before
'''
def in_signals_cart(conn, px_number):
    
    ''' 
    Returns a boolean that indicates whether a px is already in vz_safety_programs_staging.signals_cart
    
    Params
    conn: connection to PG server
    px_number: the 'px' value in each json object from the source URL
    '''
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT EXISTS(SELECT 1 FROM vz_safety_programs_staging.signals_cart WHERE asset_type = 'Traffic Signals' AND px = %s)", (px_number,))
            return cur.fetchone()[0]

def pull_traffic_signal(conn):
    
    url = "https://secure.toronto.ca/opendata/cart/traffic_signals/v3?format=json"
    return_json = requests.get(url).json()
    
    rows = []

    # column names in the PG table
    col_names = ['asset_type','px','main_street','midblock_route','side1_street','side2_street','latitude','longitude','activation_date','details']

    # attribute names in JSON dict
    att_names = ['px', 'main', 'mid_block', 'side1', 'side2', 'lat', 'long', 'activation_date', 'additional_info']

    # each "info" is all the properties of one APS, including its coords
    
    for obj in return_json:

        if not in_signals_cart(conn, obj['px']):
            
            # temporary list of properties of one TS to be appended into the rows list
            one_ts = []

            one_ts.append('Traffic Signals') # append the asset_name as listed in EC2

            # append the values in the same order as in the table
            for attr in att_names:
                one_ts.append(obj[attr])

            rows.append(tuple(one_ts))
    
    # insert into the local table if there is at least one new entry of traffic signals
    if rows:
        insert = """INSERT INTO vz_safety_programs_staging.signals_cart ({columns}) VALUES %s"""
        insert_query = sql.SQL(insert).format(
            columns = sql.SQL(',').join([
                sql.Identifier(col_names[0]),
                sql.Identifier(col_names[1]),
                sql.Identifier(col_names[2]),
                sql.Identifier(col_names[3]),
                sql.Identifier(col_names[4]),
                sql.Identifier(col_names[5]),
                sql.Identifier(col_names[6]),
                sql.Identifier(col_names[7]),
                sql.Identifier(col_names[8]),
                sql.Identifier(col_names[9])
            ]))

        with conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, rows)
    # --------------------------------------------------------------------------
    '''
    Key: column names in the json
    Value: column names in gis.traffic_signal
    '''
    
    # Decide between two lists vs one dictionary, try both of them out
    # Put the table in readme
    matched_names = {
        'px': 'px',
        'abc': 'def'
        
    }
    
    complete_rows = []
    
    column_names = []
    attribute_names = []
    
    for obj in return_json:
        one_complete_ts = []
        for attr in attribute_names:
            one_complete_ts.append(obj[attr])
            
        complete_rows.append(one_complete_ts)
    
    # Upsert query to update gis.traffic_signal
    
                
# ------------------------------------------------------------------------------
# Set up the dag and task
TRAFFIC_SIGNALS_DAG = DAG(
    'traffic_signals_dag',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    template_searchpath=[os.path.join(AIRFLOW_ROOT, 'assets/rlc/airflow/tasks')],
    schedule_interval='0 4 * * 1-5')
    # minutes past each hour | Hours (0-23) | Days of the month (1-31) | Months (1-12) | Days of the week (0-7, Sunday represented as either/both 0 and 7)

PULL_RLC = PythonOperator(
    task_id='pull_rlc',
    python_callable=pull_rlc,
    dag=TRAFFIC_SIGNALS_DAG,
    op_args=[conn]
)

PULL_APS = PythonOperator(
    task_id='pull_aps',
    python_callable=pull_aps,
    dag=TRAFFIC_SIGNALS_DAG,
    op_args=[conn]
)

PULL_PXO = PythonOperator(
    task_id='pull_pxo',
    python_callable=pull_pxo,
    dag=TRAFFIC_SIGNALS_DAG,
    op_args=[conn]
)

PULL_LPI = PythonOperator(
    task_id='pull_lpi',
    python_callable=pull_lpi,
    dag=TRAFFIC_SIGNALS_DAG,
    op_args=[conn]
)

PULL_TS = PythonOperator(
    task_id='pull_ts',
    python_callable=pull_traffic_signal,
    dag=TRAFFIC_SIGNALS_DAG,
    op_args=[conn]
)
