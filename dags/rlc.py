"""
Pipeline for pulling Red Light Camera data from Open Data API in json format via
https://secure.toronto.ca/opendata/cart/red_light_cameras.json (see
https://secure.toronto.ca/opendata/cart/red_light_cameras/details.html). This
json file will be stored in the existing table 'vz_safety_programs_staging.rlc'
in the bigdata RDS (table will be truncated each time this script is called).
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

# Credentials
from airflow.hooks.postgres_hook import PostgresHook
vz_cred = PostgresHook("vzbigdata") # name of Conn Id defined in UI
vz_pg_uri = vz_cred.get_uri() # connection to RDS for psql via BashOperator
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

    # each "info" is all the properties of one RLC, including its coords
    for info in rlcs:
        # temporary list of properties of one RLC to be appended into the rows list
        one_rlc = []

        # dive deeper into the json objects
        properties = info['properties']
        geom = info['geometry']
        coords = geom['coordinates']

        # append the values in the same order as in the table

        # column names in the PG table
        col_names = ['rlc','tcs','loc','additional_info','main','side1','side2','mid_block','private_access','x','y','district','ward1','ward2','ward3','ward4','police_division_1','police_division_2','police_division_3','date_installed','longitude','latitude'] 

        # attribute names in JSON dict
        att_names = ['RLC','TCS','NAME','ADDITIONAL_INFO','MAIN','SIDE1','SIDE2','MID_BLOCK','PRIVATE_ACCESS','X','Y','DISTRICT','WARD_1','WARD_2','WARD_3','WARD_4','POLICE_DIVISION_1','POLICE_DIVISION_2','POLICE_DIVISION_3','ACTIVATION_DATE'] 
        for attr in att_names:
            one_rlc.append(properties[attr])
        one_rlc += coords # or just coords if it's already a list of just these two elements

        rows.append(one_rlc)
    
    # truncate and insert into the local table
    insert = 'INSERT INTO {} ({}) VALUES %s'.format(local_table, ','.join(col_names))
    with conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE {}".format(local_table))
            execute_values(cur, insert, rows)
            print(rows)

# ------------------------------------------------------------------------------
# Set up the dag and task
RLC_DAG = DAG(
    'rlc_dag',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    template_searchpath=[os.path.join(AIRFLOW_ROOT, 'assets/rlc/airflow/tasks')],
    schedule_interval='0 4 * * 1-5')
    # minutes past each hour | Hours (0-23) | Days of the month (1-31) | Months (1-12) | Days of the week (0-7, Sunday represented as either/both 0 and 7)

PULL_RLC = PythonOperator(
    task_id='pull_rlc',
    python_callable=pull_rlc,
    dag=RLC_DAG,
    op_args=[conn]
)

# To run ONE DAG only:
# airflow test rlc_dag pull_rlc 29/08/2019

# https://airflow.apache.org/concepts.html?highlight=what%20dag#bitshift-composition
PULL_RLC
