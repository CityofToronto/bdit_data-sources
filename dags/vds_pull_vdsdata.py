import os
import sys
from airflow.sdk import dag, task_group, task, Variable, Variable
from datetime import datetime, timedelta
from functools import partial

from airflow.sdk import dag, task_group, task, Variable, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskMarker

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_owners import owners

from volumes.vds.py.vds_functions import (
    pull_raw_vdsdata, pull_detector_inventory, pull_entity_locations, pull_commsdeviceconfig
)
from airflow3_bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, slack_alert_data_quality, get_readme_docmd
from airflow3_bdit_dag_utils.utils.custom_operators import SQLCheckOperatorWithReturnValue
from airflow3_bdit_dag_utils.utils.common_tasks import check_jan_1st, wait_for_weather_timesensor

DAG_NAME = 'vds_pull_vdsdata'
DAG_OWNERS = owners.get(DAG_NAME, ['Unknown'])

README_PATH = os.path.join(repo_path, 'volumes/vds/readme.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True, #Allow for progressive longer waits between retries
    'on_failure_callback': partial(task_fail_slack_alert, use_proxy = True),
    'catchup': True,
}

@dag(
    dag_id = DAG_NAME,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath=[
        os.path.join(repo_path,'volumes/vds/sql'),
        os.path.join(repo_path,'dags/sql')
    ],
    doc_md=DOC_MD,
    tags=["bdit_data-sources", 'vds', 'vdsdata', 'data_checks', 'data_pull', 'detector_inventory'],
    schedule='0 4 * * *' #daily at 4am
)
def vdsdata_dag():
    @task_group
    def update_inventories():
        """This task group pulls the detector inventory and locations into bigdata.
        The vdsvehicledata DAG is also triggered after the lookups are updated."""
  
        @task(retries = 2, retry_delay = timedelta(hours=1))
        def pull_and_insert_detector_inventory():
            "Get vdsconfig from ITSC and insert into RDS `vds.vdsconfig`"
            itsc_bot = PostgresHook('itsc_postgres')
            vds_bot = PostgresHook('vds_bot')

            pull_detector_inventory(rds_conn = vds_bot, itsc_conn=itsc_bot)

        @task(retries = 2, retry_delay = timedelta(hours=1))
        def pull_and_insert_entitylocations():
            "Get entitylocations from ITSC and insert into RDS `vds.entity_locations`"
            itsc_bot = PostgresHook('itsc_postgres')
            vds_bot = PostgresHook('vds_bot')
            pull_entity_locations(rds_conn = vds_bot, itsc_conn=itsc_bot)

        @task(retries = 2, retry_delay = timedelta(hours=1))
        def pull_and_insert_commsdeviceconfig():
            "Get commsdeviceconfig from ITSC and insert into RDS `vds.config_comms_device`"
            itsc_bot = PostgresHook('itsc_postgres')
            vds_bot = PostgresHook('vds_bot')
            pull_commsdeviceconfig(rds_conn = vds_bot, itsc_conn=itsc_bot)

        t_done = ExternalTaskMarker(
                task_id="done",
                external_dag_id="vds_pull_vdsvehicledata",
                external_task_id="starting_point"
        )

        [
            pull_and_insert_detector_inventory(),
            pull_and_insert_entitylocations(),
            pull_and_insert_commsdeviceconfig()
        ] >> t_done

    @task_group
    def check_partitions():
        """Task group checks if all necessary partitions exist and
        if not executes create functions."""

        create_partitions = SQLExecuteQueryOperator(
            task_id='create_partitions',
            pre_execute=check_jan_1st,
            sql=[#partition by year and month:
                "SELECT vds.partition_vds_yyyymm('raw_vdsdata_div8001'::text, '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int, 'dt'::text)",
                "SELECT vds.partition_vds_yyyymm('raw_vdsdata_div2'::text, '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int, 'dt'::text)",
                #partition by year only: 
                "SELECT vds.partition_vds_yyyy('counts_15min_div2'::text, '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int)",
                "SELECT vds.partition_vds_yyyy('counts_15min_bylane_div2'::text, '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int)"],
            conn_id='vds_bot',
            autocommit=True
        )

        #check if Jan 1, if so trigger partition creates.
        create_partitions

    @task_group
    def pull_vdsdata():
        """Task group deletes any existing data from RDS vds.raw_vdsdata
        and then pulls and inserts from ITSC."""

        #deletes data from vds.raw_vdsdata
        delete_raw_vdsdata_task = SQLExecuteQueryOperator(
            sql="""DELETE FROM vds.raw_vdsdata
                    WHERE
                    dt >= '{{ds}} 00:00:00'::timestamp
                    AND dt < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'""",
            task_id='delete_vdsdata',
            conn_id='vds_bot',
            autocommit=True,
            retries=1,
            trigger_rule='none_failed'
        )

        @task(task_id='pull_raw_vdsdata', retries = 2, retry_delay = timedelta(hours=1))
        def pull_raw_vdsdata_task(ds=None):
            "Get vdsdata from ITSC, transform, and insert into RDS `vds.raw_vdsdata`."
            itsc_bot = PostgresHook('itsc_postgres')
            vds_bot = PostgresHook('vds_bot')

            pull_raw_vdsdata(rds_conn = vds_bot, itsc_conn = itsc_bot, start_date = ds)

        delete_raw_vdsdata_task >> pull_raw_vdsdata_task()

    @task_group
    def summarize_v15():
        """Task group deletes any existing data from RDS summary tables
        (vds.volumes_15min, vds.volumes_15min_bylane, vds.volumes_daily) and then inserts
        into the same tables."""
        
        #first deletes and then inserts summarized data into RDS `vds.counts_15min`
        summarize_v15_task = SQLExecuteQueryOperator(
            sql=["delete/delete-counts_15min.sql", "insert/insert_counts_15min.sql"],
            task_id='summarize_v15',
            conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #first deletes and then inserts summarized data into RDS `vds.counts_15min_bylane`
        summarize_v15_bylane_task = SQLExecuteQueryOperator(
            sql=["delete/delete-counts_15min_bylane.sql", "insert/insert_counts_15min_bylane.sql"],
            task_id='summarize_v15_bylane',
            conn_id='vds_bot',
            autocommit=True,
            retries=1
        )
        
        #first deletes and then inserts summarized data into RDS `vds.volumes_daily`
        summarize_volmes_daily_task = SQLExecuteQueryOperator(
            sql=["delete/delete-volumes_daily.sql", "insert/insert_volumes_daily.sql"],
            task_id='summarize_volmes_daily',
            conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        (summarize_v15_task, summarize_v15_bylane_task) >> summarize_volmes_daily_task
    
    t_done = ExternalTaskMarker(
        task_id="done",
        external_dag_id="vds_check",
        external_task_id="starting_point"
    )

    @task_group
    def data_checks():
        "Data quality checks which may warrant re-running the DAG."
        
        divisions = [2, ] #div8001 is never summarized and the query on the view is not optimized
        for divid in divisions:
            check_avg_rows = SQLCheckOperatorWithReturnValue(
                on_failure_callback=partial(slack_alert_data_quality, use_proxy=True),
                task_id=f"check_rows_vdsdata_div{divid}",
                sql="select-row_count_lookback.sql",
                conn_id='vds_bot',
                params={"table": f'vds.counts_15min_div{divid}',
                        "lookback": '60 days',
                        "dt_col": 'datetime_15min',
                        "col_to_sum": 'num_obs',
                        "threshold": 0.7},
                retries=0,
            )
            wait_for_weather_timesensor() >> check_avg_rows

    [
        [
            update_inventories(),
            check_partitions()
        ] >>
        pull_vdsdata() >>
        summarize_v15() >> t_done >>
        data_checks()
    ]

vdsdata_dag()