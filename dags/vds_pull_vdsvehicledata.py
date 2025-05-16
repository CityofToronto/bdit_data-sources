import os
import sys
import pendulum

from airflow.decorators import dag, task_group, task
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from functools import partial
from airflow.sensors.external_task import ExternalTaskSensor

DAG_NAME = 'vds_pull_vdsvehicledata'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ['Unknown'])

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)

from volumes.vds.py.vds_functions import pull_raw_vdsvehicledata
from airflow3_bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, slack_alert_data_quality, get_readme_docmd
from airflow3_bdit_dag_utils.utils.custom_operators import SQLCheckOperatorWithReturnValue
from airflow3_bdit_dag_utils.utils.common_tasks import check_jan_1st, wait_for_weather_timesensor

README_PATH = os.path.join(repo_path, 'volumes/vds/readme.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2021, 11, 1, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
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
        os.path.join(repo_path,'volumes/vds/sql'),
        os.path.join(repo_path,'dags/sql')
    ],
    doc_md=DOC_MD,
    tags=["bdit_data-sources", 'vds', 'vdsvehicledata', 'data_checks', 'data_pull'],
    schedule='5 4 * * *' #daily at 4:05am
)
def vdsvehicledata_dag():

    t_upstream_done = ExternalTaskSensor(
        task_id="starting_point",
        external_dag_id="vds_pull_vdsdata",
        external_task_id="update_inventories.done",
        poke_interval=3600, #retry hourly
        mode="reschedule",
        timeout=86400, #one day
        execution_delta=timedelta(minutes=5)
    )

    #this task group checks if all necessary partitions exist and if not executes create functions.
    @task_group(group_id='check_partitions')
    def check_partitions_TG():

        create_partitions = SQLExecuteQueryOperator(
            task_id='create_partitions',
            pre_execute=check_jan_1st,
            sql="SELECT vds.partition_vds_yyyymm('raw_vdsvehicledata', '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int, 'dt')",
            conn_id='vds_bot',
            autocommit=True
        )
        
        #check if Jan 1, if so trigger partition creates.
        create_partitions

    #this task group deletes any existing data from `vds.raw_vdsvehicledata` and then pulls and inserts from ITSC into RDS
    @task_group
    def pull_vdsvehicledata():

        #deletes data from vds.raw_vdsvehicledata
        delete_vdsvehicledata_task = SQLExecuteQueryOperator(
            sql="""DELETE FROM vds.raw_vdsvehicledata
                    WHERE
                    dt >= '{{ds}} 00:00:00'::timestamp
                    AND dt < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'""",
            task_id='delete_vdsvehicledata',
            conn_id='vds_bot',
            autocommit=True,
            retries=1,
            trigger_rule='none_failed'
        )

        #get vdsvehicledata from ITSC and insert into RDS `vds.raw_vdsvehicledata`
        @task(task_id='pull_raw_vdsvehicledata', retries = 2, retry_delay = timedelta(hours=1))
        def pull_raw_vdsvehicledata_task(ds=None):
            itsc_bot = PostgresHook('itsc_postgres')
            vds_bot = PostgresHook('vds_bot')

            pull_raw_vdsvehicledata(rds_conn = vds_bot, itsc_conn = itsc_bot, start_date = ds)

        delete_vdsvehicledata_task >> pull_raw_vdsvehicledata_task()

    @task_group
    def summarize_vdsvehicledata():
        """This task group summarizes vdsvehicledata into `vds.veh_speeds_15min`
        (5km/h speed bins), `vds.veh_length_15min` (1m length bins)"""

        #deletes from and then inserts new data into summary table vds.aggregate_15min_veh_speeds
        summarize_speeds_task = SQLExecuteQueryOperator(
            sql=["delete/delete-veh_speeds_15min.sql", "insert/insert_veh_speeds_15min.sql"],
            task_id='summarize_speeds',
            conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #deletes from and then insert new data into summary table vds.veh_length_15min
        summarize_lengths_task = SQLExecuteQueryOperator(
            sql=["delete/delete-veh_length_15min.sql", "insert/insert_veh_lengths_15min.sql"],
            task_id='summarize_lengths',
            conn_id='vds_bot',
            autocommit=True,
            retries=1
        )
        
        summarize_speeds_task
        summarize_lengths_task

    @task_group(group_id='data_checks')
    def data_checks():
        "Data quality checks which may warrant re-running the DAG."

        check_avg_rows = SQLCheckOperatorWithReturnValue(
            on_failure_callback=slack_alert_data_quality,
            task_id=f"check_rows_veh_speeds",
            sql="select-row_count_lookback.sql",
            conn_id='vds_bot',
            params={"table": 'vds.veh_speeds_15min',
                    "lookback": '60 days',
                    "dt_col": 'datetime_15min',
                    "col_to_sum": 'count',
                    "threshold": 0.7},
            retries=2
        )
        wait_for_weather_timesensor() >> check_avg_rows

    [t_upstream_done, check_partitions_TG()] >> pull_vdsvehicledata() >> summarize_vdsvehicledata() >> data_checks()

vdsvehicledata_dag()