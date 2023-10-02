"""
Pipeline to pull Watch Your Speed sign data data and put them into the wys.raw_data table using Python Operator.
A Slack notification is raised when the airflow process fails.
"""
import os
import sys
from functools import partial
import pendulum

from datetime import timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.decorators import task, dag

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from googleapiclient.discovery import build

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from wys.api.python.wys_api import api_main, get_schedules
    from wys.api.python.wys_google_sheet import read_masterlist
    from dags.dag_functions import task_fail_slack_alert
except:
    raise ImportError("Cannot import functions to pull watch your speed data")

def custom_fail_slack_alert(context: dict) -> str:
    """Adds a custom failure message in case of failing to pull data of some wards.

    Args:
        context: The calling Airflow task's context

    Returns:
        str: A string containing a custom message to get attached to the 
            standard failure alert.
    """
    empty_wards = context.get(
        "task_instance"
    ).xcom_pull(
        task_ids=context.get("task_instance").task_id,
        key="empty_wards"
    )
    if empty_wards:
        return (
            "Failed to pull/load the data of the following wards: " + 
            ", ".join(map(str, empty_wards))
        )
    else:
        return ""

dag_name = 'pull_wys'

dag_owners = Variable.get('dag_owners', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2020, 4, 1, tz="America/Toronto"),
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 3,
                 'retry_delay': timedelta(minutes=5),
                 #progressive longer waits between retries
                 'retry_exponential_backoff': True,
                 'on_failure_callback': task_fail_slack_alert
                }

@dag(dag_id = dag_name,
     default_args=default_args,
     schedule_interval='0 15 * * *' # Run at 3 PM local time every day
     )
def pull_wys_dag():

    @task
    def pull_wys(ds=None):
        #to connect to pgadmin bot
        wys_postgres = PostgresHook("wys_bot")
        
        #api connection
        api_conn = BaseHook.get_connection('wys_api_key')
        api_key = api_conn.password

        with wys_postgres.get_conn() as conn:
            api_main(start_date = ds,
                     end_date = ds,
                     conn = conn,
                     api_key=api_key)

    @task
    def pull_schedules():
        #to connect to pgadmin bot
        wys_postgres = PostgresHook("wys_bot")
        
        #api connection
        api_conn = BaseHook.get_connection('wys_api_key')
        api_key = api_conn.password

        with wys_postgres.get_conn() as conn:
            get_schedules(conn, api_key)
    
    @task(on_failure_callback = partial(
                    task_fail_slack_alert, extra_msg=custom_fail_slack_alert
                    ))
    def read_google_sheets():
        #to connect to pgadmin bot
        wys_postgres = PostgresHook("wys_bot")

        #to get credentials to access google sheets
        wys_api_hook = GoogleBaseHook('vz_api_google')
        cred = wys_api_hook.get_credentials()
        service = build('sheets', 'v4', credentials=cred, cache_discovery=False)

        with wys_postgres.get_conn() as con:
            read_masterlist(con, service)

    pull_wys()
    pull_schedules()
    read_google_sheets()

pull_wys_dag()