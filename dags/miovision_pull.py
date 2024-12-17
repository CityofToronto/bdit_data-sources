r"""### Daily Miovision Data Pull DAG
Pipeline to pull miovision daily data and insert them into Postgres tables using Python Operator.
Inserted data is also aggregated into 15 minute and daily summaries, and unacceptable_gaps.
Also creates new yearly/monthly partition tables if necessary and runs SQL data-checks on the
pulled data, checking row count and distinct classification_uids compared to a lookback period.
"""
import sys
import os
import pendulum
from datetime import timedelta
import configparser
import dateutil.parser

from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.macros import ds_add

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert, get_readme_docmd
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
    from dags.common_tasks import check_jan_1st, check_1st_of_month, wait_for_weather_timesensor
    from volumes.miovision.api.intersection_tmc import (
        run_api, find_gaps, aggregate_15_min_mvt, aggregate_15_min, aggregate_volumes_daily,
        get_report_dates, get_intersection_info, agg_zero_volume_anomalous_ranges
    )
    from volumes.miovision.api.pull_alert import run_alerts_api
except:
    raise ImportError("Cannot import DAG helper functions.")

DAG_NAME = 'miovision_pull'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

README_PATH = os.path.join(repo_path, 'volumes/miovision/api/readme.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

API_CONFIG_PATH = '/data/airflow/data_scripts/volumes/miovision/api/config.cfg'

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 9, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule='0 3 * * *',
    template_searchpath=os.path.join(repo_path,'dags/sql'),
    catchup=False,
    params={
        "intersection": Param(
            default=[0],
            type="array",
            title="An array of intersection_uid (integers).",
            description="A list of intersection_uid to pull/aggregate for a single date. Default [0] will pull all intersections.",
            items={"type": "number"},
        )
    },
    tags=["miovision", "data_pull", "partition_create", "data_checks"],
    doc_md=DOC_MD
)
def pull_miovision_dag():

    @task_group(tooltip="Tasks to check if necessary to create new partitions and if so, exexcute.")
    def check_partitions():

        create_annual_partition = SQLExecuteQueryOperator(
            task_id='create_annual_partitions',
            pre_execute=check_jan_1st,
            sql=["SELECT miovision_api.create_yyyy_volumes_partition('volumes', '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int, 'datetime_bin')",
                 "SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min', '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int)",
                 "SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt', '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int)"],
            conn_id='miovision_api_bot',
            autocommit=True
        )
      
        create_month_partition = SQLExecuteQueryOperator(
            task_id='create_month_partition',
            pre_execute=check_1st_of_month,
            sql="""SELECT miovision_api.create_mm_nested_volumes_partitions('volumes'::text, '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int, '{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}'::int)""",
            conn_id='miovision_api_bot',
            autocommit=True,
            trigger_rule='none_failed_min_one_success'
        )

        create_annual_partition >> create_month_partition

    @task(trigger_rule='none_failed', retries = 1)
    def pull_miovision(ds = None, **context):
        if context["params"]["intersection"] == [0]:
            INTERSECTION = ()
        else:
            INTERSECTION = tuple(context["params"]["intersection"])
        
        run_api(
            start_date=ds,
            end_date=ds_add(ds, 1),
            intersection=INTERSECTION,
            pull=True,
            agg=False
        )

    @task(trigger_rule='none_failed', retries = 1)
    def pull_alerts(ds):
        run_alerts_api(
            start_date=ds,
            end_date=ds_add(ds, 1)
        )

    @task_group(tooltip="Tasks to aggregate newly pulled Miovision data.")
    def miovision_agg():
        @task
        def find_gaps_task(ds = None, **context):
            mio_postgres = PostgresHook("miovision_api_bot")
            time_period = (ds, ds_add(ds, 1))
            #no user specified intersection
            if context["params"]["intersection"] == [0]:
                with mio_postgres.get_conn() as conn:
                    find_gaps(conn, time_period=time_period)
            #user specified intersection
            else:
                INTERSECTIONS = tuple(context["params"]["intersection"])
                with mio_postgres.get_conn() as conn:
                    intersections = get_intersection_info(conn, intersection=INTERSECTIONS)
                    find_gaps(conn, time_period=time_period, intersections=intersections)

        @task
        def aggregate_15_min_mvt_task(ds = None, **context):
            mio_postgres = PostgresHook("miovision_api_bot")
            time_period = (ds, ds_add(ds, 1))
            #no user specified intersection
            if context["params"]["intersection"] == [0]:
                with mio_postgres.get_conn() as conn:
                    aggregate_15_min_mvt(conn, time_period=time_period)
            #user specified intersection
            else:
                INTERSECTIONS = tuple(context["params"]["intersection"])
                with mio_postgres.get_conn() as conn:
                    intersections = get_intersection_info(conn, intersection=INTERSECTIONS)
                    aggregate_15_min_mvt(conn, time_period=time_period, intersections=intersections)

        @task
        def zero_volume_anomalous_ranges_task(ds = None, **context):
            mio_postgres = PostgresHook("miovision_api_bot")  
            time_period = (ds, ds_add(ds, 1))          
            #no user specified intersection
            if context["params"]["intersection"] == [0]:
                with mio_postgres.get_conn() as conn:
                    agg_zero_volume_anomalous_ranges(conn, time_period=time_period)
            #user specified intersection
            else:
                INTERSECTIONS = tuple(context["params"]["intersection"])              
                with mio_postgres.get_conn() as conn:
                    intersections = get_intersection_info(conn, intersection=INTERSECTIONS)
                    agg_zero_volume_anomalous_ranges(conn, time_period=time_period, intersections=intersections)

        @task
        def aggregate_15_min_task(ds = None, **context):
            mio_postgres = PostgresHook("miovision_api_bot")
            time_period = (ds, ds_add(ds, 1))
            #no user specified intersection
            if context["params"]["intersection"] == [0]:
                with mio_postgres.get_conn() as conn:
                    aggregate_15_min(conn, time_period=time_period)
            #user specified intersection
            else:
                INTERSECTIONS = tuple(context["params"]["intersection"])
                with mio_postgres.get_conn() as conn:
                    intersections = get_intersection_info(conn, intersection=INTERSECTIONS)
                    aggregate_15_min(conn, time_period=time_period, intersections=intersections)

        @task
        def aggregate_volumes_daily_task(ds = None, **context):
            mio_postgres = PostgresHook("miovision_api_bot")  
            time_period = (ds, ds_add(ds, 1))          
            #no user specified intersection
            if context["params"]["intersection"] == [0]:
                with mio_postgres.get_conn() as conn:
                    aggregate_volumes_daily(conn, time_period=time_period)
            #user specified intersection
            else:
                INTERSECTIONS = tuple(context["params"]["intersection"])              
                with mio_postgres.get_conn() as conn:
                    intersections = get_intersection_info(conn, intersection=INTERSECTIONS)
                    aggregate_volumes_daily(conn, time_period=time_period, intersections=intersections)

        @task
        def get_report_dates_task(ds = None, **context):
            mio_postgres = PostgresHook("miovision_api_bot")
            time_period = (ds, ds_add(ds, 1))
            #no user specified intersection
            if context["params"]["intersection"] == [0]:
                with mio_postgres.get_conn() as conn:
                    get_report_dates(conn, time_period=time_period)
            #user specified intersection
            else:
                INTERSECTIONS = tuple(context["params"]["intersection"])              
                with mio_postgres.get_conn() as conn:
                    intersections = get_intersection_info(conn, intersection=INTERSECTIONS)
                    get_report_dates(conn, time_period=time_period, intersections=intersections)

        find_gaps_task() >> aggregate_15_min_mvt_task() >> [aggregate_15_min_task(), zero_volume_anomalous_ranges_task()] >> aggregate_volumes_daily_task()
        get_report_dates_task()

    t_done = ExternalTaskMarker(
            task_id="done",
            external_dag_id="miovision_check",
            external_task_id="starting_point"
    )

    @task_group(tooltip="Tasks to check critical data quality measures which could warrant re-running the DAG.")
    def data_checks():
        data_check_params = {
            "table": "miovision_api.volumes_15min_mvt",
            "lookback": '60 days',
            "dt_col": 'datetime_bin',
            "threshold": 0.7
        }
        check_row_count = SQLCheckOperatorWithReturnValue(
            task_id="check_row_count",
            sql="select-row_count_lookback.sql",
            conn_id="miovision_api_bot",
            retries=0,
            params=data_check_params | {"col_to_sum": 'volume'},
        )
        check_row_count.doc_md = '''
        Compare the row count today with the average row count from the lookback period.
        '''

        check_distinct_classification_uid = SQLCheckOperatorWithReturnValue(
            task_id="check_distinct_classification_uid",
            sql="select-sensor_id_count_lookback.sql",
            conn_id="miovision_api_bot",
            retries=0,
            params=data_check_params | {
                    "id_col": "classification_uid",
                    "threshold": 0.999 #dif is floored, so this will catch a dif of 1. 
                },
        )
        check_distinct_classification_uid.doc_md = '''
        Compare the count of classification_uids appearing in today's pull vs the lookback period.
        '''

        wait_for_weather_timesensor() >> check_row_count
        check_distinct_classification_uid

    (
        check_partitions() >>
        [pull_miovision(), pull_alerts()] >>
        miovision_agg() >>
        t_done >>
        data_checks()
    )

pull_miovision_dag()