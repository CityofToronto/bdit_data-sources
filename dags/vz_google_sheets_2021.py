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

from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed. 
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
        # proxy='http://137.15.73.132:8080'
        )
    return failed_alert.execute(context=context)

#to read the python script for pulling data from google sheet and putting it into tables in postgres
try:
  # sys.path.append('/etc/airflow/data_scripts/vision_zero/')
  # from schools import pull_from_sheet
  sys.path.append('/home/cnangini/PROJECTS/bdit_data-sources/vision_zero/')
  from schools_2021 import pull_from_sheet
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
    'owner': 'cnangini',
    'depends_on_past' : False,
    'email': ['cathy.nangini@toronto.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime(2019, 9, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

dag = DAG('vz_google_sheets', default_args=DEFAULT_ARGS, schedule_interval='@daily', catchup=False)

# task1 = PythonOperator(
#     task_id='2018',
#     python_callable=pull_from_sheet,
#     dag=dag,
#     op_args=[con, service, 2018]
#     )
# 
# task2 = PythonOperator(
#     task_id='2019',
#     python_callable=pull_from_sheet,
#     dag=dag,
#     op_args=[con, service, 2019]
#     )
#     
# task3 = PythonOperator(
#     task_id='2020',
#     python_callable=pull_from_sheet,
#     dag=dag,
#     op_args=[con, service, 2020]
#     )
    
task4 = PythonOperator(
    task_id='2021',
    python_callable=pull_from_sheet,
    dag=dag,
    op_args=[con, service, 2021]
    )
