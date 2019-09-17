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
import psycopg2
import requests
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

AIRFLOW_DAGS = os.path.dirname(os.path.realpath(__file__))
AIRFLOW_ROOT = os.path.dirname(AIRFLOW_DAGS)
AIRFLOW_TASKS = os.path.join(AIRFLOW_ROOT, 'assets/rlc/airflow/tasks')

DEFAULT_ARGS = {
    'email': ['Cathy.Nangini@toronto.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'owner': 'airflow',
    'start_date': datetime(2019, 9, 16), # YYYY, MM, DD
    'task_concurrency': 1
}

# ------------------------------------------------------------------------------
def pull_rlc():
  '''
  Connect to bigdata RDS, pull Red Light Camera json file from Open Data API,
  and overwrite existing rlc table in the vz_safety_programs_staging schema.
  '''
  db_name='bigdata'
  username='vzairflow'
  host='10.160.12.47'
  local_table='vz_safety_programs_staging.rlc_test'
  # Connect to bigdata RDS
  conn = psycopg2.connect(database=db_name, user=username, host=host, port=5432)

  url = "https://secure.toronto.ca/opendata/cart/red_light_cameras.json"
  return_json = requests.get(url).json()
  rows = [list(feature.values()) for feature in return_json]
  insert = 'INSERT INTO {0} VALUES %s'.format(local_table)
  with conn:
    with conn.cursor() as cur:
      execute_values(cur, insert, rows)
      print(rows)

  conn.close()

# ------------------------------------------------------------------------------
# Set up the dag and task
RLC_DAG = DAG(
    'rlc_dag',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    template_searchpath=[os.path.join(AIRFLOW_ROOT, 'assets/rlc/airflow/tasks')],
    schedule_interval='0 4 * * 1-5')
    # minutes past each hour | Hours (0-23) | Days of the month (1-31) | Months (1-12) | Days of the week (0-7, Sunday represented as either/both 0 and 7)

TRUNCATE_TABLE = BashOperator(
    task_id='truncate_rlc',
    bash_command="/truncate_rlc.sh",
    dag=RLC_DAG
)

PULL_RLC = PythonOperator(
    task_id='pull_rlc',
    python_callable=pull_rlc,
    dag=RLC_DAG
)

# To run ONE DAG only:
# airflow test rlc_dag pull_rlc 29/08/2019
#

# https://airflow.apache.org/concepts.html?highlight=what%20dag#bitshift-composition
TRUNCATE_TABLE >> PULL_RLC
