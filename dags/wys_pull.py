"""
Pipeline to pull Watch Your Speed sign data data and put them into the wys.raw_data table using Python Operator.
A Slack notification is raised when the airflow process fails.
"""
import os
import sys
from functools import partial
import pendulum
import dateutil.parser
from datetime import timedelta

from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.decorators import task, dag, task_group
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.exceptions import AirflowFailException

from googleapiclient.discovery import build

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from wys.api.python.wys_api import (
        get_schedules, agg_1hr_5kph, get_location_ids, get_api_key,
        get_data_for_date, update_locations
    )
    from wys.api.python.wys_google_sheet import read_masterlist, pull_from_sheet
    from dags.dag_functions import task_fail_slack_alert, get_readme_docmd
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
    from dags.common_tasks import check_jan_1st, check_1st_of_month
except:
    raise ImportError("Cannot import functions to pull watch your speed data")

DAG_NAME = 'wys_pull'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

README_PATH = os.path.join(repo_path, 'wys/api/README.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past':False,
    'start_date': pendulum.datetime(2024, 8, 6, tz="America/Toronto"),
    'email_on_failure': False,
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
    schedule='0 17 * * *', # Run at 5:00 PM local time every day
    tags=["wys", "data_pull", "partition_create", "data_checks", "google_sheets"],
    doc_md=DOC_MD
)
def pull_wys_dag():

    #this task group checks if necessary to create new partitions and if so, exexcute.
    @task_group
    def check_partitions():

        create_annual_partition = PostgresOperator(
            task_id='create_annual_partitions',
            pre_execute=check_jan_1st,
            sql="SELECT wys.create_yyyy_raw_data_partition('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int)",
            postgres_conn_id='wys_bot',
            autocommit=True
        )
        
        create_month_partition = PostgresOperator(
            task_id='create_month_partition',
            pre_execute=check_1st_of_month,
            trigger_rule='none_failed_min_one_success',
            sql="SELECT wys.create_mm_nested_raw_data_partitions('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int, '{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}'::int)",
            postgres_conn_id='wys_bot',
            autocommit=True
        )

        create_annual_partition >> create_month_partition
    
    @task_group()
    def api_pull():

        @task(trigger_rule='none_failed')
        def signs():
            api_key = get_api_key()
            location_ids = get_location_ids(api_key = api_key)
            return location_ids
        
        @task(retries = 0)
        def pull_wys(location_ids, ds=None):
            #to connect to pgadmin bot
            wys_postgres = PostgresHook("wys_bot")
            api_key = get_api_key()
            
            with wys_postgres.get_conn() as conn:
                locations = get_data_for_date(ds, location_ids, api_key, conn)
                return locations
        
        @task(retries = 0)
        def update_wys_locations(locations):
            #to connect to pgadmin bot
            wys_postgres = PostgresHook("wys_bot")
            
            with wys_postgres.get_conn() as conn:
                update_locations(locations, conn)
        
        location_ids = signs()
        locations = pull_wys(location_ids)
        update_wys_locations(locations)
    
    @task()
    def agg_speed_counts_hr(ds=None):
        wys_postgres = PostgresHook("wys_bot")
        start_date = dateutil.parser.parse(ds).date()
        end_date = start_date + timedelta(days=1)
        with wys_postgres.get_conn() as conn:
            agg_1hr_5kph(start_date, end_date, conn)

    t_done = ExternalTaskMarker(
        task_id="done",
        external_dag_id="wys_check",
        external_task_id="starting_point"
    )

    @task_group()
    def data_checks():
        check_row_count = SQLCheckOperatorWithReturnValue(
            task_id="check_row_count",
            sql="select-row_count_lookback.sql",
            conn_id="wys_bot",
            retries=0,
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
        api_key = get_api_key()

        with wys_postgres.get_conn() as conn:
            get_schedules(conn, api_key)

    @task
    def read_masterlist():
        wys_postgres = PostgresHook("wys_bot")
        ward_list = []
        with wys_postgres.get_conn() as conn, conn.cursor() as cur:
            read_table="SELECT spreadsheet_id, range_name, schema_name, ward_no FROM wys.ward_masterlist ORDER BY ward_no"
            cur.execute(read_table)
            ward_list=cur.fetchall()
            return ward_list
    
    @task(retries = 1, on_failure_callback = None, map_index_template="{{ ward_no }}")
    def read_google_sheets(ward):
        #name mapped task
        context = get_current_context()
        context["ward_no"] = f"ward_{ward[3]}"

        #to connect to pgadmin bot
        wys_postgres = PostgresHook("wys_bot")

        #to get credentials to access google sheets
        wys_api_hook = GoogleBaseHook('google_sheets_api')
        cred = wys_api_hook.get_credentials()
        service = build('sheets', 'v4', credentials=cred, cache_discovery=False)
        with wys_postgres.get_conn() as conn:
            if not pull_from_sheet(conn, service, ward):
                return ward[3]

    @task()
    def sheets_status_msg():
        if empty_wards != []:
            failure_msg = "Failed to pull/load the data of the following wards: " + ", ".join(map(str, empty_wards))
            context.get("task_instance").xcom_push(key="extra_msg", value=failure_msg)
            raise AirflowFailException(failure_msg)

    check_partitions() >> api_pull() >> agg_speed_counts_hr() >> t_done >> data_checks()
    pull_schedules()
    
    wards = read_masterlist()
    read_google_sheets.expand(ward=wards)

pull_wys_dag()
