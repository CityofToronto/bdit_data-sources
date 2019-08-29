"""
Pipeline for pulling traffic signal data from Oracle database in Traffic Control group
"""
from datetime import datetime
import os
import sys
import psycopg2
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# # since `pull_traffic_signal_functions.py` does not exist in this dir
# # we need to put a try block around it so the linter wont think its an error
# try:
#   sys.path.append('../assets/traffic_signals/tasks/pull_traffic_signals/')
#   from pull_traffic_signal_functions import add_geometry, create_tables, insert_into_table
# except:
#   raise ImportError("Cannot import functions to pull traffic signals")

AIRFLOW_DAGS = os.path.dirname(os.path.realpath(__file__))
AIRFLOW_ROOT = os.path.dirname(AIRFLOW_DAGS)
AIRFLOW_TASKS = os.path.join(AIRFLOW_ROOT, 'assets/traffic_signals/airflow/tasks')

DEFAULT_ARGS = {
    'email': ['Cathy.Nangini@toronto.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'owner': 'airflow',
    'start_date': datetime(2019, 7, 9),
    'task_concurrency': 1
}

TRAFFIC_SIGNAL_DAG = DAG(
    'traffic_signals',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    schedule_interval='10 6-22 * * 1-5')


CREATE_SIGNAL_QUERY_TABLES_SH = os.path.join(AIRFLOW_TASKS, 'traffic_signals.sh')
CREATE_SIGNAL_QUERY_TABLES = BashOperator(
    task_id='traffic_signals',
    bash_command='{0} '.format(CREATE_SIGNAL_QUERY_TABLES_SH),
    dag=TRAFFIC_SIGNAL_DAG
)
