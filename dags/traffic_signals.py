"""
Pipeline for pulling traffic signal data from Oracle database in Traffic Control
group. A view called 'signals_cart' is already set up in the 'traffic_signals'
database in the local PostgreSQL server on the BDIT Linux box. The view was
created using Foreign Data Wrappers to allow querying the Oracle tables with
PostgreSQL commands.
"""
from datetime import datetime
import os
import sys
import psycopg2
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.oracle_operator import OracleOperator
from airflow.operators.postgres_operator import PostgresOperator

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
TEST_DAG = DAG(
    'pg_bash',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    template_searchpath=[os.path.join(AIRFLOW_ROOT, 'assets/traffic_signals/airflow/tasks')],
    schedule_interval='10 6-22 * * 1-5')

RUN_BASH_SQL = BashOperator(
    task_id='call_signalscart',
    bash_command="/test.sh",
    dag=TEST_DAG
)

# To run:
# airflow test pg_bash call_signalscart 29/08/2019

# ------------------------------------------------------------------------------
GPA2_DAG = DAG(
    'gpa2_dag',
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    template_searchpath=[os.path.join(AIRFLOW_ROOT, 'assets/traffic_signals/airflow/tasks')],
    schedule_interval='10 6-22 * * 1-5')

SELECT_VZ_SMP = OracleOperator(
    task_id='gpa2_sql',
    oracle_conn_id='gcc_gpa2',
    sql='/vz_safetymeasurepoint.sql',
    dag=GPA2_DAG
)

# To run:
# airflow test gpa2_dag gpa2_sql 29/08/2019
