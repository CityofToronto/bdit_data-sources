"""
Pipeline to pull here data every week and put them into the here.ta table using Bash Operator.
Slack notifications is raised when the airflow process fails.
"""
import json
import sys
import os
import pendulum

from airflow import DAG
from airflow.decorators import task, dag
from datetime import datetime, timedelta
from airflow.models.connection import Connection
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable 

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
    from here.traffic.here_api_test import query_dates, get_access_token, get_download_url, download_data, send_data_to_database
    cfg_path = repo_path
except:
    raise ImportError("Cannot import slack alert functions")

dag_name = 'pull_here_path'

dag_owners = Variable.get('dag_owners', deserialize_json=True)
names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

here_postgres = PostgresHook("here_bot")
rds_con = here_postgres.get_uri()

default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2023, 9, 26, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0, #Retry 3 times
                'retry_delay': timedelta(minutes=60), #Retry after 60 mins
                'retry_exponential_backoff': True, #Allow for progressive longer waits between retries
                'on_failure_callback': task_fail_slack_alert,
                'env':{'here_bot':rds_con,
                       'LC_ALL':'C.UTF-8', #Necessary for Click
                       'LANG':'C.UTF-8'}
                }


@dag(dag_id = dag_name,
     default_args=default_args,
     schedule_interval='30 10 * * *' ,
     catchup=False,
     tags=["HERE"]
     )

def pull_here_path():

    @task
    def parse_date(**kwargs):
        ds = kwargs["ds"]
        # Get the day before execution date
        pull_date = (datetime.strptime(ds, '%Y-%m-%d').date() - timedelta(days=1)).strftime("%Y%m%d")
        return pull_date

    @task
    def send_request(pull_date: str):
        api_conn = BaseHook.get_connection('here_api_key')
        access_token = get_access_token(api_conn.password, api_conn.extra_dejson['client_secret'], api_conn.extra_dejson['token_url'])
        return access_token

    @task
    def get_request_id(pull_date: str, access_token: str):
        api_conn = BaseHook.get_connection('here_api_key')
        request_id = query_dates(access_token, pull_date, pull_date, api_conn.host, api_conn.login, api_conn.extra_dejson['user_email'])
        return request_id
    
    @task(retries=2) 
    def get_download_link(request_id: str, access_token: str, pull_date: str):
        api_conn = BaseHook.get_connection('here_api_key')
        download_url = get_download_url(request_id, api_conn.extra_dejson['status_base_url'], access_token, api_conn.login)
        return download_url
    
    pull_date = parse_date()
    access_token = send_request(pull_date)
    request_id =  get_request_id(pull_date, access_token)
    download_url = get_download_link(request_id, access_token, pull_date)

    load_data_run = BashOperator(
        task_id = "load_data",
        bash_command = '''curl $DOWNLOAD_URL | gunzip | psql -h $HOST -U $USER -d bigdata -c "\COPY here.ta_path_view FROM STDIN WITH (FORMAT csv, HEADER TRUE);" ''', 
        env = {"DOWNLOAD_URL": download_url, 
               "HOST":  BaseHook.get_connection("here_bot").host, 
               "USER" :  BaseHook.get_connection("here_bot").login, 
               "PGPASSWORD": BaseHook.get_connection("here_bot").password},
        append_env=True
    )

    load_data_run

pull_here_path()