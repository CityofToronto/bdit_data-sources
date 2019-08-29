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
AIRFLOW_TASKS = os.path.join(AIRFLOW_ROOT, '../assets/traffic_signals/airflow/tasks')


default_args = {
    'email': ['Cathy.Nangini@toronto.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'owner': 'airflow',
    'start_date': datetime(2019, 7, 9),
    'task_concurrency': 1
}

dag = DAG(dag_id='my_custom_dag',
          default_args=default_args,
          schedule_interval='10 6-22 * * 1-5')

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)
