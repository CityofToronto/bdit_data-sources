"""
Pipeline for pulling traffic signal data from Oracle database in Traffic Control
group. A view called 'signals_cart' is already set up in the public schema of the
'traffic_signals' database in the local PostgreSQL server on the BDIT Linux box.
The view was created in pgAdmin from SIGNALVIEW Oracle tables using Foreign Data
Wrappers.
"""
from datetime import datetime
import os
import sys
import psycopg2
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

AIRFLOW_DAGS = os.path.dirname(os.path.realpath(__file__))
AIRFLOW_ROOT = os.path.dirname(AIRFLOW_DAGS)
AIRFLOW_TASKS = os.path.join(AIRFLOW_ROOT, 'assets/traffic_signals/airflow/tasks')

DEFAULT_ARGS = {
    'email': ['Cathy.Nangini@toronto.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'owner': 'airflow',
    'start_date': datetime(2019, 7, 9), # YYYY, MM, DD
    'task_concurrency': 1
}

# ------------------------------------------------------------------------------
TRAFFIC_DAG = DAG(
    'traffic_dag',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    template_searchpath=[os.path.join(AIRFLOW_ROOT, 'assets/traffic_signals/airflow/tasks')],
    schedule_interval='0 4 * * 1-5')    
    # minutes past each hour | Hours (0-23) | Days of the month (1-31) | Months (1-12) | Days of the week (0-7, Sunday represented as either/both 0 and 7)

COPY_VIEW = BashOperator(
    task_id='copy_signalscart',
    bash_command="/copy_signalscart.sh",
    dag=TRAFFIC_DAG
)

# To run:
# airflow test traffic_dag copy_signalscart 29/08/2019
