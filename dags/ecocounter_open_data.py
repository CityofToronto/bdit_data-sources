r"""### Monthly ecocounter Open Data DAG
Pipeline to run monthly ecocounter aggregations for Open Data.
"""
import sys
import os
from datetime import timedelta
import logging
import pendulum
from functools import partial

from airflow.decorators import dag, task, task_group
from airflow.models import Variable 
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.date_time import DateTimeSensor
from airflow.macros import ds_format


try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert, send_slack_msg
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
except:
    raise ImportError("Cannot import DAG helper functions.")

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

DAG_NAME = 'ecocounter_open_data'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past':False,
    #set earlier start_date + catchup when ready?
    'start_date': pendulum.datetime(2024, 10, 1, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': partial(task_fail_slack_alert, use_proxy = True),
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule='0 11 1 * *', # 10am, 1st day of each month
    catchup=True,
    max_active_runs=1,
    tags=["ecocounter", "open_data"],
    doc_md=__doc__
)
def ecocounter_open_data_dag():

    t_upstream_done = ExternalTaskSensor(
        task_id="starting_point",
        external_dag_id="ecocounter_pull",
        external_task_id="done",
        poke_interval=3600, #retry hourly
        mode="reschedule",
        doc_md="Wait for last day of month to run before running monthly DAG.",
        timeout=86400, #one day
        #wait for the 1st of the following month
        execution_date_fn=lambda dt: dt + pendulum.duration(months=1, hours=-1) #ecocounter_pull scheduled at '0 10 * * *'
    )

    check_data_availability = SQLCheckOperatorWithReturnValue(
        task_id="check_data_availability",
        sql="""WITH daily_volumes AS (
            SELECT dt::date, COALESCE(SUM(daily_volume), 0) AS daily_volume
            FROM generate_series('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date,
                                 '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date + '1 month'::interval - '1 day'::interval,
                                 '1 day'::interval) AS dates(dt)
            LEFT JOIN ecocounter.open_data_daily_counts USING (dt)
            GROUP BY dt
            ORDER BY dt
        )

        SELECT NOT(COUNT(*) > 0), 'Missing dates: ' || string_agg(dt::text, ', ')
        FROM daily_volumes
        WHERE daily_volume = 0""",
        conn_id="ecocounter_bot"
    )

    @task(retries=0, doc_md="""A reminder message.""")
    def reminder_message(ds = None, **context):
        mnth = ds_format(ds, '%Y-%m-%d', '%Y-%m')
        slack_ids = Variable.get("slack_member_id", deserialize_json=True)
        list_names = " ".join([slack_ids.get(name, name) for name in DAG_OWNERS])

        send_slack_msg(
            context=context,
            msg=f"{list_names} Remember to check Ecocounter :open_data_to: for {mnth} and label any sites pending validation in anomalous_ranges. :meow_detective:",
            use_proxy=True
        )

    wait_till_10th = DateTimeSensor(
        task_id="wait_till_10th",
        timeout=10*86400,
        mode="reschedule",
        poke_interval=3600*24,
        target_time="{{ next_execution_date.replace(day=10) }}",
    )
    wait_till_10th.doc_md = """
    Wait until the 10th day of the month to export data. Alternatively mark task as success to proceed immediately.
    """

    @task_group()
    def insert_and_download_data():
        insert_daily = PostgresOperator(
            sql="SELECT ecocounter.open_data_daily_counts_insert({{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }})",
            task_id='insert_daily_open_data',
            postgres_conn_id='ecocounter_bot',
            autocommit=True,
            retries = 0
        )
        insert_raw = PostgresOperator(
            sql="SELECT ecocounter.open_data_raw_counts_insert({{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }})",
            task_id='insert_raw_open_data',
            postgres_conn_id='ecocounter_bot',
            autocommit=True,
            retries = 0
        )

        @task.bash(env={
            "HOST":  BaseHook.get_connection("ecocounter_bot").host,
            "USER" :  BaseHook.get_connection("ecocounter_bot").login,
            "PGPASSWORD": BaseHook.get_connection("ecocounter_bot").password
        })
        def download_daily_open_data()->str:
            return '''psql -h $HOST -U $USER -d bigdata -c \
                "SELECT * FROM ecocounter.open_data_daily_counts WHERE dt >= date_trunc('year'::text, '{{ ds }}'::date) LIMIT 100" \
                --csv -o /data/open_data/permanent-bike-counters/ecocounter_daily_counts_{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}.csv'''
            
        @task.bash(env={
            "HOST":  BaseHook.get_connection("ecocounter_bot").host,
            "USER" :  BaseHook.get_connection("ecocounter_bot").login,
            "PGPASSWORD": BaseHook.get_connection("ecocounter_bot").password
        })
        def download_raw_open_data()->str:
            return '''psql -h $HOST -U $USER -d bigdata -c \
                "SELECT * FROM ecocounter.open_data_raw_counts WHERE datetime_bin >= date_trunc('year'::text, '{{ ds }}'::date) LIMIT 100" \
                --csv -o /data/open_data/permanent-bike-counters/ecocounter_raw_counts_{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}.csv'''
    
        insert_daily >> download_daily_open_data()
        insert_raw >> download_raw_open_data()

    @task(
        retries=0,
        trigger_rule='all_success',
        doc_md="""A status message to report DAG success."""
    )
    def status_message(ds = None, **context):
        mnth = ds_format(ds, '%Y-%m-%d', '%Y-%m-01')
        send_slack_msg(
            context=context,
            msg=f"Ecocounter :open_data_to: DAG ran successfully for {mnth} :white_check_mark:",
            use_proxy=True
        )

    (
        t_upstream_done >>
        check_data_availability >>
        reminder_message() >>
        wait_till_10th >> 
        insert_and_download_data() >>
        status_message()
    )

ecocounter_open_data_dag()