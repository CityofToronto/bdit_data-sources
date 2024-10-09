"""
Pipeline for pulling various types of Traffic Signals data from Open Data API in json format.
Information about the types of traffic signals and the tables that store them is listed below.

vz_safety_programs_staging.rlc
- Red Light Cameras (RLC)

vz_safety_programs_staging.signals_cart
- Pedestrian Head Start Signals/Leading Pedestrian Intervals (LPI)
- Accessible Pedestrian Signals (APS)
- Pedestrian Crossovers (PXO)
- Traffic Signals

gis.traffic_signal
- Traffic Signals

Updated on 2022-09-12
"""

import os
import sys
from psycopg2 import sql
import requests
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable 

from dateutil.parser import parse
from datetime import datetime
import pendulum

dag_name = 'traffic_signals_dag'

# Credentials
from airflow.providers.postgres.hooks.postgres import PostgresHook
vz_cred = PostgresHook("vz_api_bot") # name of Conn Id defined in UI
#vz_pg_uri = vz_cred.get_uri() # connection to RDS for psql via BashOperator

# ------------------------------------------------------------------------------
# Slack notification
repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_functions import task_fail_slack_alert

dag_owners = Variable.get('dag_owners', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    


# ------------------------------------------------------------------------------
AIRFLOW_DAGS = os.path.dirname(os.path.realpath(__file__))
AIRFLOW_ROOT = os.path.dirname(AIRFLOW_DAGS)
AIRFLOW_TASKS = os.path.join(AIRFLOW_ROOT, 'assets/rlc/airflow/tasks')

DEFAULT_ARGS = {
    'email': ['Cathy.Nangini@toronto.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'owner': 'airflow',
    'start_date': pendulum.datetime(2019, 9, 16, tz="America/Toronto"), # YYYY, MM, DD
    'max_active_tis_per_dag': 1,
    'on_failure_callback': task_fail_slack_alert
}

# ------------------------------------------------------------------------------
def pull_rlc():
    '''
    Connect to bigdata RDS, pull Red Light Camera json file from Open Data API,
    and overwrite existing rlc table in the vz_safety_programs_staging schema.
    '''
    conn = vz_cred.get_conn() # connection to RDS for python via PythonOperator
    
    local_table='vz_safety_programs_staging.rlc'
    url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/9fcff3e1-3737-43cf-b410-05acd615e27b/resource/7e4ac806-4e7a-49d3-81e1-7a14375c9025/download/Red%20Light%20Cameras%20Data.geojson"  
    return_json = requests.get(url).json()
    
    rlcs = return_json['features']
    rows = []
    
    # column names in the PG table
    col_names = ['rlc','tcs','loc','additional_info','main','side1','side2','mid_block','private_access','district','ward1','ward2','ward3','ward4','police_division_1','police_division_2','police_division_3','date_installed','longitude','latitude'] 

    # attribute names in JSON dict
    att_names = ['RLC','TCS','NAME','ADDITIONAL_INFO','MAIN','SIDE1','SIDE2','MID_BLOCK','PRIVATE_ACCESS','DISTRICT','WARD_1','WARD_2','WARD_3','WARD_4','POLICE_DIVISION_1','POLICE_DIVISION_2','POLICE_DIVISION_3','ACTIVATION_DATE'] 

    # each "info" is all the properties of one RLC, including its coords
    for row_no, info in enumerate(rlcs, 1):
        # temporary list of properties of one RLC to be appended into the rows list
        one_rlc = []

        # dive deeper into the json objects
        properties = info['properties']
        geom = info['geometry']
        coords = geom['coordinates']

        # append the values in the same order as in the table
        for attr in att_names:
            one_rlc.append(properties[attr])
        if isinstance(coords[0], list) \
            and isinstance(coords[0][0], float) \
            and isinstance(coords[0][1], float):
            one_rlc += coords[0]
        # elif len(coords) == 2 and isinstance(coords[0], float) and isinstance(coords[1], float):
        #     one_rlc += coords
        else:
            raise TypeError(f'Invalid coordinates type at row {row_no}: {coords}')

        rows.append(tuple(one_rlc))
    
    # truncate and insert into the local table
    insert = """INSERT INTO vz_safety_programs_staging.rlc ({columns}) VALUES %s"""
    insert_query = sql.SQL(insert).format(
        columns = sql.SQL(',').join([sql.Identifier(col) for col in col_names])
    )
    
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
        columns = sql.SQL(',').join([sql.Identifier(col) for col in col_names])
    )
    
    delete = """DELETE FROM vz_safety_programs_staging.signals_cart WHERE asset_type = %s"""
    delete_query = sql.SQL(delete)
    
    with conn:
        with conn.cursor() as cur:
            cur.execute(delete_query, (ts_type,))
            execute_values(cur, insert_query, rows)

# ------------------------------------------------------------------------------
def pull_aps():
    
    '''
    Pulls Accessible Pedestrian Signals
    '''
    
    conn = vz_cred.get_conn() # connection to RDS for python via PythonOperator
    
    # table is 'vz_safety_programs_staging.signals_cart'
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
def pull_pxo():
    '''
    Pulls Pedestrian Crossovers
    '''
    
    conn = vz_cred.get_conn() # connection to RDS for python via PythonOperator
    
    # table is 'vz_safety_programs_staging.signals_cart'
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

def pull_lpi():
    '''
    Pulls Pedestrian Head Start Signals/Leading Pedestrian Intervals
    '''
    
    conn = vz_cred.get_conn() # connection to RDS for python via PythonOperator
    
    # table is 'vz_safety_programs_staging.signals_cart'
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
def get_point_geometry(long, lat):
    
    '''
    Returns a string that will be converted to geometry in postGIS
    
    Params
    long: value of 'long' in traffic_signals/v3 json
    lat: value of 'lat' in traffic_signals/v3 json
    '''
    return 'SRID=4326;Point('+(str(long))+' '+ (str(lat))+')'  
        
def pull_traffic_signal():
    '''
    This function would pull all records from https://secure.toronto.ca/opendata/cart/traffic_signals/v3?format=json
    into the bigdata database. One copy will be in vz_safety_programs_staging.signals_cart while another will be in
    gis.traffic_signal
    '''
    
    conn = vz_cred.get_conn() # connection to RDS for python via PythonOperator
    # --------------------------------------------------------------------------
    '''
    Delete previous records of asset_type = 'Traffic Signals' and then insert all records from Open API 
    into vz_safety_programs_staging.signals_cart
    '''
    
    url = "https://secure.toronto.ca/opendata/cart/traffic_signals/v3?format=json"
    return_json = requests.get(url).json()
    
    rows = []

    # column names in the PG table
    col_names = ['asset_type','px','main_street','midblock_route','side1_street','side2_street','latitude','longitude','activation_date','details']

    # attribute names in JSON dict
    att_names = ['px', 'main', 'mid_block', 'side1', 'side2', 'lat', 'long', 'activation_date', 'additional_info']

    # each "info" is all the properties of one APS, including its coords
    
    for obj in return_json:
        #do not add Temporary (portable) traffic signals to vz_safety_programs_staging.signals_cart
        if identify_temp_signals(obj['px']) == 'Temporary (portable)':
            continue
        # temporary list of properties of one TS to be appended into the rows list
        one_ts = []
        one_ts.append('Traffic Signals') # append the asset_name as listed in EC2
        # append the values in the same order as in the table
        for attr in att_names:
            one_ts.append(obj[attr])
        rows.append(tuple(one_ts))
    
    # delete existing Traffic Signals and insert into the local table
    insert_data(conn, col_names, rows, 'Traffic Signals')
    
    # --------------------------------------------------------------------------
    ''' 
    Upsert into gis.traffic_signal, an audited table
    '''
    
    complete_rows = []
    
    # column names in the PG table
    column_names = ['px', 'main_street', 'midblock_route', 'side1_street', 'side2_street', 'private_access', 'additional_info', 'x', 'y', 'latitude', 'longitude',
                    'activationdate', 'signalsystem', 'non_system', 'control_mode', 'pedwalkspeed', 'aps_operation', 'numberofapproaches', 'geo_id', 'node_id',
                    'audiblepedsignal', 'transit_preempt', 'fire_preempt', 'rail_preempt', 'bicycle_signal', 'ups', 'led_blankout_sign',
                    'lpi_north_implementation_date', 'lpi_south_implementation_date', 'lpi_east_implementation_date', 'lpi_west_implementation_date', 'lpi_comment',
                    'aps_activation_date', 'leading_pedestrian_intervals', 'geom']
    
    # attribute names in JSON dict
    attribute_names = ['px', 'main', 'mid_block', 'side1', 'side2', 'private_access', 'additional_info', 'x', 'y', 'lat', 'long',
                      'activation_date', 'signal_system', 'non_system', 'mode_of_control', 'ped_walk_speed', 'aps_operation', 'no_of_signalized_approaches', 'geo_id', 'node_id',
                      'aps_signal', 'transit_preempt', 'fire_preempt', 'rail_preempt', 'bicycle_signal', 'ups', 'ledBlankoutSign',
                      'lpiNorthImplementationDate', 'lpiSouthImplementationDate', 'lpiEastImplementationDate', 'lpiWestImplementationDate', 'lpiComment',
                      'aps_activation_date', 'leading_pedestrian_intervals']
    
    for obj in return_json:
        one_complete_ts = []
        for attr in attribute_names:
            one_complete_ts.append(obj[attr])
        # Create point geometry based on x and y
        one_complete_ts.append(get_point_geometry(obj['long'], obj['lat']))
        
        complete_rows.append(one_complete_ts)
    
    # Upsert query to update gis.traffic_signal
    upsert_gis = """INSERT INTO gis.traffic_signal ({columns}) VALUES %s ON CONFLICT (px) DO UPDATE SET """
    for col in column_names:
        upsert_gis += (col + '=EXCLUDED.' + col + ',')
    upsert_gis = upsert_gis.rstrip(",") # delete the trailing comma

    upsert_query_gis = sql.SQL(upsert_gis).format(
        columns = sql.SQL(',').join([sql.Identifier(col) for col in column_names])
    )

    with conn:
        with conn.cursor() as cur:
            execute_values(cur, upsert_query_gis, complete_rows)
    
# ------------------------------------------------------------------------------
# Set up the dag and task
TRAFFIC_SIGNALS_DAG = DAG(
    dag_id = dag_name,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    template_searchpath=[os.path.join(AIRFLOW_ROOT, 'assets/rlc/airflow/tasks')],
    schedule='0 4 * * 1-5')
    # minutes past each hour | Hours (0-23) | Days of the month (1-31) | Months (1-12) | Days of the week (0-7, Sunday represented as either/both 0 and 7)

PULL_RLC = PythonOperator(
    task_id='pull_rlc',
    python_callable=pull_rlc,
    dag=TRAFFIC_SIGNALS_DAG
)

PULL_APS = PythonOperator(
    task_id='pull_aps',
    python_callable=pull_aps,
    dag=TRAFFIC_SIGNALS_DAG
)

PULL_PXO = PythonOperator(
    task_id='pull_pxo',
    python_callable=pull_pxo,
    dag=TRAFFIC_SIGNALS_DAG
)

PULL_LPI = PythonOperator(
    task_id='pull_lpi',
    python_callable=pull_lpi,
    dag=TRAFFIC_SIGNALS_DAG
)

PULL_TS = PythonOperator(
    task_id='pull_ts',
    python_callable=pull_traffic_signal,
    dag=TRAFFIC_SIGNALS_DAG
)
