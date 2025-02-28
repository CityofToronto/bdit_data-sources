import os
import sys
from functools import partial
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

DAG_NAME = 'bluetooth_itsc_pull'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ['Unknown'])

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)

from bluetooth.itsc.bluetooth_itsc import (
    fetch_and_insert_raw_tt_data, fetch_and_insert_tt_path_data, fetch_and_insert_raw_tt_pathdata
)
from dags.dag_functions import task_fail_slack_alert, get_readme_docmd

README_PATH = os.path.join(repo_path, 'bluetooth/itsc/readme.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True, #Allow for progressive longer waits between retries
    'on_failure_callback': partial(task_fail_slack_alert, use_proxy = True),
    'catchup': True,
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath=[
        os.path.join(repo_path,'bluetooth/itsc/sql')
    ],
    doc_md=DOC_MD,
    tags=['bdit_data-sources', 'bluetooth', 'pull', 'itsc_central'],
    schedule='0 4 * * *' #daily at 4am
)

def bt_itsc_dag():
    @task
    def pull_raw_tt_data(ds = None):
        "Fetches data from ITS Central traveltimepathdata table, inserts into RDS gwolofs.tt_raw_pathdata table."
        itsc_bot = PostgresHook('itsc_postgres')
        events_bot = PostgresHook('events_bot')
        fetch_and_insert_raw_tt_data(start_date=ds, select_conn=itsc_bot, insert_conn=events_bot)
        
    @task
    def pull_tt_paths(ds = None):
        "Fetches data from ITS Central traveltimepathconfig table, inserts into RDS gwolofs.tt_paths table."
        itsc_bot = PostgresHook('itsc_postgres')
        events_bot = PostgresHook('events_bot')
        fetch_and_insert_tt_path_data(start_date=ds, select_conn=itsc_bot, insert_conn=events_bot)
    
    @task
    def pull_raw_tt_pathdata(ds = None):
        "Fetches data from ITS Central traveltimepathrawdata table, inserts into RDS gwolofs.tt_raw table."
        itsc_bot = PostgresHook('itsc_postgres')
        events_bot = PostgresHook('events_bot')
        fetch_and_insert_raw_tt_pathdata(start_date=ds, select_conn=itsc_bot, insert_conn=events_bot)

    #these tasks are not dependent, but this helps so only one fails at a time
    pull_raw_tt_data() >> pull_raw_tt_pathdata() >> pull_tt_paths()
    
bt_itsc_dag()
