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
from airflow.decorators import task, dag, task_group

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from googleapiclient.discovery import build

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from wys.api.python.wys_api import api_main, get_schedules
    from wys.api.python.wys_google_sheet import read_masterlist
    from dags.dag_functions import task_fail_slack_alert
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
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
     catchup=False,
     max_active_runs=5,
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

    @task_group()
    def data_checks():
        data_check_params = {
            "table": "wys.raw_data",
            "lookback": '60 days',
            "dt_col": 'datetime_bin',
            "threshold": 0.7
        }
        check_row_count = SQLCheckOperatorWithReturnValue(
            task_id="check_row_count",
            sql="wys/api/sql/select-row_count_lookback.sql",
            conn_id="wys_bot",
            params=data_check_params,
            retries=2
        )
        check_distinct_api_id = SQLCheckOperatorWithReturnValue(
            task_id="check_distinct_api_id",
            sql="wys/api/sql/select-api_id_count_lookback.sql",
            conn_id="wys_bot",
            params=data_check_params | {"sensor_id_col": "api_id"},
            retries=2
        )

        check_row_count
        check_distinct_api_id

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

        read_masterlist(wys_postgres.get_conn(), service)

    pull_wys() >> data_checks()
    pull_schedules()
    read_google_sheets()

pull_wys_dag()
