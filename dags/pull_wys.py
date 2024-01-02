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
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.decorators import task, dag, task_group
from airflow.sensors.external_task import ExternalTaskMarker

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from googleapiclient.discovery import build

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from wys.api.python.wys_api import api_main, get_schedules
    from wys.api.python.wys_google_sheet import read_masterlist
    from dags.dag_functions import task_fail_slack_alert
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
    from dags.common_tasks import check_jan_1st, check_1st_of_month
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

DAG_NAME = 'pull_wys'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

default_args = {
    'owner': ','.join(DAG_OWNERS),
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

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    catchup=False,
    max_active_runs=5,
    template_searchpath=os.path.join(repo_path,'dags/sql'),
    schedule='0 15 * * *', # Run at 3 PM local time every day
    tags=["wys", "data_pull", "partition_create", "data_checks"],
    doc_md=__doc__
)
def pull_wys_dag():

    #this task group checks if necessary to create new partitions and if so, exexcute.
    @task_group
    def check_partitions():

        create_annual_partition = PostgresOperator(
            task_id='create_annual_partitions',
            sql="SELECT wys.create_yyyy_raw_data_partition('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int)",
            postgres_conn_id='wys_bot',
            autocommit=True
        )
        
        create_month_partition = PostgresOperator(
            task_id='create_month_partition',
            sql="SELECT wys.create_mm_nested_raw_data_partitions('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int, '{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}'::int)",
            postgres_conn_id='wys_bot',
            autocommit=True
        )

        check_jan_1st() >> create_annual_partition >> (
            check_1st_of_month() >> create_month_partition
        )

    @task(trigger_rule='none_failed')
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
    
    t_done = ExternalTaskMarker(
        task_id="done",
        external_dag_id="check_miovision",
        external_task_id="starting_point"
    )

    @task_group()
    def data_checks():
        check_row_count = SQLCheckOperatorWithReturnValue(
            task_id="check_row_count",
            sql="select-row_count_lookback.sql",
            conn_id="wys_bot",
            retries=1,
            params={
                "table": "wys.speed_counts_agg_5kph",
                "lookback": '60 days',
                "dt_col": 'datetime_bin',
                "col_to_sum": 'volume',
                "threshold": 0.7
            }
        )

        check_row_count

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
    def read_google_sheets(**kwargs):
        #to connect to pgadmin bot
        wys_postgres = PostgresHook("wys_bot")

        #to get credentials to access google sheets
        wys_api_hook = GoogleBaseHook('vz_api_google')
        cred = wys_api_hook.get_credentials()
        service = build('sheets', 'v4', credentials=cred, cache_discovery=False)

        read_masterlist(wys_postgres.get_conn(), service, **kwargs)


    check_partitions() >> pull_wys() >> t_done >> data_checks()
    pull_schedules()
    read_google_sheets()

pull_wys_dag()
