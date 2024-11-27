r"""### Monthly ecocounter Open Data DAG
Pipeline to run monthly ecocounter aggregations for Open Data.
"""
import sys
import os
from datetime import timedelta, datetime
import logging
import pendulum
from functools import partial

from airflow.decorators import dag, task, task_group
from airflow.models import Variable 
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.date_time import DateTimeSensor
from airflow.macros import ds_format
from airflow.operators.python import get_current_context

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert, send_slack_msg, get_readme_docmd
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
except:
    raise ImportError("Cannot import DAG helper functions.")

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

DAG_NAME = 'ecocounter_open_data'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

README_PATH = os.path.join(repo_path, 'volumes/ecocounter/readme.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past':False,
    #set earlier start_date + catchup when ready?
    'start_date': pendulum.datetime(2024, 1, 1, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': partial(task_fail_slack_alert, use_proxy = True),
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule='0 12 1 * *', # 12pm, 1st day of each month
    catchup=False,
    max_active_runs=1,
    tags=["ecocounter", "open_data"],
    doc_md=DOC_MD
)
def ecocounter_open_data_dag():

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
    
    @task()
    def get_years(ds=None):
        mnth = pendulum.from_format(ds, 'YYYY-MM-DD')
        prev_mnth = mnth.subtract(months=1)
        yrs = [str(mnth.year), str(prev_mnth.year)]
        return list(set(yrs)) #unique

    @task_group()
    def insert_and_download_data(yr):
        @task(map_index_template="{{ yr }}")
        def insert_daily(yr):
            context = get_current_context()
            context["yr"] = yr
            t = PostgresOperator(
                sql=f"SELECT ecocounter.open_data_daily_counts_insert({yr}::int)",
                task_id='insert_daily_open_data',
                postgres_conn_id='ecocounter_bot',
                autocommit=True,
                retries = 0
            )
            return t.execute(context=context)

        @task(map_index_template="{{ yr }}")
        def insert_15min(yr):
            context = get_current_context()
            context["yr"] = yr
            t = PostgresOperator(
                sql=f"SELECT ecocounter.open_data_15min_counts_insert({yr}::int)",
                task_id='insert_15min_open_data',
                postgres_conn_id='ecocounter_bot',
                autocommit=True,
                retries = 0
            )
            return t.execute(context=context)
        
        @task.bash(
            map_index_template="{{ yr }}",
            env={
                "HOST":  BaseHook.get_connection("ecocounter_bot").host,
                "USER" :  BaseHook.get_connection("ecocounter_bot").login,
                "PGPASSWORD": BaseHook.get_connection("ecocounter_bot").password
            }
        )
        def download_daily_open_data(yr)->str:
            context = get_current_context()
            context["yr"] = yr
            return f'''/usr/bin/psql -h $HOST -U $USER -d bigdata -c \
                "SELECT location_name, direction, dt, daily_volume
                FROM open_data.cycling_permanent_counts_daily
                WHERE
                    dt >= to_date({yr}::text, 'yyyy')
                    AND dt < LEAST(date_trunc('month', now()), to_date(({yr}::int+1)::text, 'yyyy'));" \
                --csv -o "/data/open_data/permanent-bike-counters/cycling_permanent_counts_daily_{yr}.csv"'''
            
        @task.bash(
            map_index_template="{{ yr }}",
            env={
                "HOST":  BaseHook.get_connection("ecocounter_bot").host,
                "USER" :  BaseHook.get_connection("ecocounter_bot").login,
                "PGPASSWORD": BaseHook.get_connection("ecocounter_bot").password
            }
        )
        def download_15min_open_data(yr)->str:
            context = get_current_context()
            context["yr"] = yr
            return f'''/usr/bin/psql -h $HOST -U $USER -d bigdata -c \
                "SELECT location_name, direction, datetime_bin, bin_volume
                FROM open_data.cycling_permanent_counts_15min
                WHERE
                    datetime_bin >= to_date({yr}::text, 'yyyy')
                    AND datetime_bin < LEAST(date_trunc('month', now()), to_date(({yr}+1)::text, 'yyyy'));" \
                --csv -o "/data/open_data/permanent-bike-counters/cycling_permanent_counts_15min_{yr}.csv"'''

        insert_daily(yr) >> download_daily_open_data(yr)
        insert_15min(yr) >> download_15min_open_data(yr)
    
    @task.bash(env={
        "HOST":  BaseHook.get_connection("ecocounter_bot").host,
        "USER" :  BaseHook.get_connection("ecocounter_bot").login,
        "PGPASSWORD": BaseHook.get_connection("ecocounter_bot").password
    })
    def download_locations_open_data()->str:
        return '''/usr/bin/psql -h $HOST -U $USER -d bigdata -c \
                "SELECT location_name, direction, linear_name_full, side_street, longitude,
                    latitude, centreline_id, bin_size, latest_calibration_study,
                    first_active, last_active, date_decommissioned, technology
                FROM open_data.cycling_permanent_counts_locations" \
                --csv -o /data/open_data/permanent-bike-counters/cycling_permanent_counts_locations.csv'''

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
    
    @task.bash()
    def output_readme()->str:
        return '''
        pandoc -V geometry:margin=1in \
            -o /data/open_data/permanent-bike-counters/cycling_permanent_counts_readme.pdf \
            volumes/open_data/sql/cycling_permanent_counts_readme.md
        '''
    yrs = get_years()
    (
        check_data_availability >>
        reminder_message() >>
        wait_till_10th >> 
        [insert_and_download_data.expand(yr = yrs), download_locations_open_data(), output_readme()] >>
        status_message()
    )
        
ecocounter_open_data_dag()