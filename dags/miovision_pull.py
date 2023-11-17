"""
Pipeline to pull miovision daily data and put them into postgres tables using Bash Operator.
Slack notifications is raised when the airflow process fails.
"""
import sys
import os
import pendulum
from datetime import datetime, timedelta
import configparser

from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.macros import ds_add

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_functions import task_fail_slack_alert
from volumes.miovision.api.intersection_tmc import run_api, find_gaps, \
    aggregate_15_min_mvt, aggregate_15_min, aggregate_volumes_daily, \
    get_report_dates, get_intersection_info

dag_name = 'miovision_pull'

dag_owners = Variable.get('dag_owners', deserialize_json=True)
names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

API_CONFIG_PATH = '/etc/airflow/data_scripts/volumes/miovision/api/config.cfg'

default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2023, 12, 1, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

@dag(dag_id=dag_name,
     default_args=default_args,
     schedule_interval='0 3 * * *',
     catchup=False,
     params={
            "intersection": Param(
                default=0,
                type="integer",
                title="A single intersection_uid.",
                description="A single intersection_uid to pull/aggregate for a single date.",
            )
        },
     tags = ['miovision']
    )
def pull_miovision_dag():
# Add 3 hours to ensure that the data are at least 2 hours old

    #this task group checks if necessary to create new partitions and if so, exexcute.
    @task_group
    def check_partitions():
        YEAR = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}'
        MONTH = '{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}'

        @task.short_circuit(ignore_downstream_trigger_rules=False) #only skip immediately downstream task
        def check_annual_partition(ds=None): #check if Jan 1 to trigger partition creates. 
            start_date = datetime.strptime(ds, '%Y-%m-%d')
            if start_date.month == 1 and start_date.day == 1:
                return True
            return False
      
        create_annual_partition = PostgresOperator(
            task_id='create_annual_partitions',
            sql=["SELECT miovision_api.create_yyyy_volumes_partition('volumes', {{ params.year }}::int, 'datetime_bin')",
                 "SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min', {{ params.year }}::int)",
                 "SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt', {{ params.year }}::int)"],
            postgres_conn_id='miovision_api_bot',
            params={"year": YEAR},
            autocommit=True
        )

        @task.short_circuit(ignore_downstream_trigger_rules=False) #only skip immediately downstream task
        def check_month_partition(ds=None): #check if 1st of Month to trigger partition creates. 
            start_date = datetime.strptime(ds, '%Y-%m-%d')
            if start_date.day == 1:
                return True
            return False
        
        create_month_partition = PostgresOperator(
            task_id='create_month_partition',
            sql="SELECT miovision_api.create_mm_nested_volumes_partition('volumes', {{ params.year }}::int, {{ params.month }}::int)",
            postgres_conn_id='miovision_api_bot',
            params={"year": YEAR,
                    "month": MONTH},
            autocommit=True
        )

        check_annual_partition() >> create_annual_partition
        check_month_partition() >> create_month_partition

    @task(trigger_rule='none_failed', retries = 1)
    def pull_miovision(ds = None, **context):
        INTERSECTION = () if context["params"]["intersection"] == 0 else context["params"]["intersection"]
        run_api(start_date=ds,
                end_date=ds_add(ds, 1),
                path=API_CONFIG_PATH,
                intersection=INTERSECTION,
                pull=True,
                dupes=True)

    @task_group
    def miovision_agg():
        @task
        def find_gaps_task(ds = None):
            mio_postgres = PostgresHook("miovision_api_bot")  
            with mio_postgres.get_conn() as conn:
                find_gaps(conn, ds, ds_add(ds, 1))

        @task
        def aggregate_15_min_mvt_task(ds = None, **context):
            mio_postgres = PostgresHook("miovision_api_bot")
            if context["params"]["intersection"] != 0:
                intersections = get_intersection_info(conn, intersection=context["params"]["intersection"])
                with mio_postgres.get_conn() as conn:
                    aggregate_15_min_mvt(conn, ds, ds_add(ds, 1), True, intersections)
            else:
                with mio_postgres.get_conn() as conn:
                    aggregate_15_min_mvt(conn, ds, ds_add(ds, 1))

        @task
        def aggregate_15_min_task(ds = None):
            mio_postgres = PostgresHook("miovision_api_bot")  
            with mio_postgres.get_conn() as conn:
                aggregate_15_min(conn, ds, ds_add(ds, 1))

        @task
        def aggregate_volumes_daily_task(ds = None):
            mio_postgres = PostgresHook("miovision_api_bot")  
            with mio_postgres.get_conn() as conn:
                aggregate_volumes_daily(conn, ds, ds_add(ds, 1))

        @task
        def get_report_dates_task(ds = None):
            mio_postgres = PostgresHook("miovision_api_bot")  
            with mio_postgres.get_conn() as conn:
                get_report_dates(conn, ds, ds_add(ds, 1))
        
        find_gaps_task() >> aggregate_15_min_mvt_task() >> [aggregate_15_min_task(), aggregate_volumes_daily_task()]
        get_report_dates_task()

    check_partitions() >> pull_miovision() >> miovision_agg()
    
pull_miovision_dag()