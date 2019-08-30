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
from airflow.operators.oracle_operator import OracleOperator
from airflow.operators.postgres_operator import PostgresOperator

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

# ------------------------------------------------------------------------------
# DAG 1 - OracleOperator
# https://stackoverflow.com/questions/53084753/how-to-execute-multiple-oracle-statements-from-sql-script-in-airflow-oracleoper
ORACLE_DAG = DAG(
    'oracle_sql',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    template_searchpath=[os.path.join(AIRFLOW_ROOT, 'assets/traffic_signals/airflow/tasks')],
    schedule_interval='10 6-22 * * 1-5')

RUN_ORACLE_SQL = OracleOperator(
    task_id='oracle_sql',
    oracle_conn_id='gcc_gpa2',
    sql='/oracle_query.sql',
    dag=ORACLE_DAG
)

# Run DAG 1 on command line:
# airflow test oracle_sql oracle_sql 29/08/2019

# ------------------------------------------------------------------------------
# DAG 2 - PostgresOperator
# Insert data into local postgres database (selon this example: https://hackersandslackers.com/managing-data-pipelines-with-apache-airflow/)

SAVE_TO_POSTGRES = PostgresOperator(
    task_id='my_postgres_task',
    sql="INSERT INTO test_table VALUES (42);",
    postgres_conn_id='local_postgres',
    autocommit=True,
    database="airflow",
    dag=ORACLE_DAG
)
# Run DAG 2 on command line:
# airflow test oracle_sql my_postgres_task 29/08/2019

# ------------------------------------------------------------------------------
# Define task order
SAVE_TO_POSTGRES.set_upstream(RUN_ORACLE_SQL)
