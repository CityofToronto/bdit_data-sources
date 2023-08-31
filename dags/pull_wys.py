"""
Pipeline to pull Watch Your Speed sign data data and put them into the wys.raw_data table using Python Operator.
A Slack notification is raised when the airflow process fails.
"""
import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable 

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from googleapiclient.discovery import build
from dateutil.relativedelta import relativedelta

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from wys.api.python.wys_api import api_main
    from wys.api.python.wys_google_sheet import read_masterlist
    from dags.dag_functions import task_fail_slack_alert
except:
    raise ImportError("Cannot import functions to pull watch your speed data")

dag_name = 'pull_wys'

dag_owners = Variable.get('dag_owners', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

#to get credentials to access google sheets
wys_api_hook = GoogleCloudBaseHook('vz_api_google')
cred = wys_api_hook._get_credentials()
service = build('sheets', 'v4', credentials=cred, cache_discovery=False)

#to connect to pgadmin bot
wys_postgres = PostgresHook("wys_bot")
connection = BaseHook.get_connection('wys_api_key')
api_key = connection.password

default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': datetime(2020, 4, 1),
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 3,
                 'retry_delay': timedelta(minutes=5),
                 #progressive longer waits between retries
                 'retry_exponential_backoff': True,
                 'on_failure_callback': task_fail_slack_alert
                }

dag = DAG(dag_id = dag_name, default_args=default_args, schedule_interval='0 15 * * *')
# Run at 3 PM local time every day

with wys_postgres.get_conn() as con:
    t1 = PythonOperator(
            task_id = 'pull_wys',
            python_callable = api_main, 
            dag = dag,
            op_kwargs = {'conn':con, 
                        'start_date':'{{ ds }}', 
                        'end_date':'{{ ds }}', 
                        'api_key':api_key}
                        )
    
    t2 = PythonOperator(
            task_id = 'read_google_sheets',
            python_callable = read_masterlist,
            dag = dag,
            op_args = [con, service]
            )
