"""
Pipeline for pulling two vz google sheets data and putting them into postgres tables using Python Operator.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import sys

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from googleapiclient.discovery import build

#to read the python script for pulling data from google sheet and putting it into tables in postgres
try:
  sys.path.append('/home/jchew/bdit_data-sources/vision_zero/')
  from schools import pull_from_sheet
except:
  raise ImportError("Cannot import functions to pull school safety zone list")

#to get credentials to access google sheets
vz_api_hook = GoogleCloudBaseHook('vz_api_google')
cred = vz_api_hook._get_credentials()
service = build('sheets', 'v4', credentials=cred, cache_discovery=False)

#To connect to pgadmin bot
vz_api_bot = PostgresHook("vz_api_bot")
con = vz_api_bot.get_conn()

DEFAULT_ARGS = {
    'owner': 'jchew',
    'depends_on_past' : False,
    'email': ['Joven.Chew@toronto.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime(2019, 9, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('vz_google_sheets', default_args=DEFAULT_ARGS, schedule_interval='@daily')

task1 = PythonOperator(
    task_id='2018',
    python_callable=pull_from_sheet,
    dag=dag,
    op_args=[con, service, 2018]
    )

task2 = PythonOperator(
    task_id='2019',
    python_callable=pull_from_sheet,
    dag=dag,
    op_args=[con, service, 2019]
    )

#run task1 and then task2 (task2 is dependent on task1 though)
task1 >> task2
