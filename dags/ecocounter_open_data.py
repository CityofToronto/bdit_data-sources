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
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.date_time import DateTimeSensor
from airflow.macros import ds_format
from airflow.operators.python import get_current_context

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert, send_slack_msg, get_readme_docmd
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
except ModuleNotFoundError:
    raise ImportError("Cannot import DAG helper functions.")
except ImportError:
    raise ImportError("Cannot import DAG helper functions.")

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

DAG_NAME = 'ecocounter_open_data'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

README_PATH = os.path.join(repo_path, 'volumes/ecocounter/readme.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)
EXPORT_PATH = '/home/airflow/open_data/permanent-bike-counters' #'/data/open_data/permanent-bike-counters'
BASH_ENV = {
    'HOST': '{{ conn.ecocounter_bot.host }}',
    'LOGIN': '{{ conn.ecocounter_bot.login }}',
    'PGPASSWORD': '{{ conn.ecocounter_bot.password }}',
    'EXPORT_PATH': EXPORT_PATH
}

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
    template_searchpath=os.path.join(repo_path,'volumes/ecocounter'),
    catchup=False,
    max_active_runs=1,
    tags=["bdit_data-sources", "ecocounter", "open_data"],
    doc_md=DOC_MD
)
def ecocounter_open_data_dag():

    check_data_availability = SQLCheckOperatorWithReturnValue(
        task_id="check_data_availability",
        sql="data_checks/select-data-availability.sql",
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
        yrs = [mnth.year, prev_mnth.year]
        return list(set(yrs)) #unique

    update_locations = SQLExecuteQueryOperator(
        sql=f"SELECT ecocounter.open_data_locations_insert()",
        task_id='update_locations',
        conn_id='ecocounter_bot',
        autocommit=True,
        retries = 0
    )

    @task_group()
    def insert_and_download_data(yr):
        @task(map_index_template="{{ yr }}")
        def insert_daily(yr):
            context = get_current_context()
            context["yr"] = yr
            t = SQLExecuteQueryOperator(
                sql=f"SELECT ecocounter.open_data_daily_counts_insert({yr}::int)",
                task_id='insert_daily_open_data',
                conn_id='ecocounter_bot',
                autocommit=True,
                retries = 0
            )
            return t.execute(context=context)

        @task(map_index_template="{{ yr }}")
        def insert_15min(yr):
            context = get_current_context()
            context["yr"] = yr
            t = SQLExecuteQueryOperator(
                sql=f"SELECT ecocounter.open_data_15min_counts_insert({yr}::int)",
                task_id='insert_15min_open_data',
                conn_id='ecocounter_bot',
                autocommit=True,
                retries = 0
            )
            return t.execute(context=context)
        
        @task.bash(env = BASH_ENV)
        def download_daily_open_data()->str:
            return '''/usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c \
                "SELECT
                    location_dir_id, location_name, direction, linear_name_full,
                    side_street, dt, daily_volume
                FROM open_data.cycling_permanent_counts_daily
                WHERE dt < LEAST(date_trunc('month', now()))
                ORDER BY location_dir_id, dt;" \
                --csv -o "$EXPORT_PATH/cycling_permanent_counts_daily.csv"'''

        @task.bash(
            env = BASH_ENV,
            map_index_template="{{ yr }}"
        )
        def download_15min_open_data(yr)->str:
            context = get_current_context()
            context["yr"] = yr
            return f'''/usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c \
                "SELECT location_dir_id, datetime_bin, bin_volume
                FROM open_data.cycling_permanent_counts_15min
                WHERE
                    datetime_bin >= to_date({yr}::text, 'yyyy')
                    AND datetime_bin < LEAST(date_trunc('month', now()), to_date(({yr}+1)::text, 'yyyy'))
                ORDER BY location_dir_id, datetime_bin;" \
                --csv -o "$EXPORT_PATH/cycling_permanent_counts_15min_{yr}_{yr+1}.csv"'''
        
        #insert only latest year data, but download everything (single file)
        insert_daily(yr) >> download_daily_open_data()
        insert_15min(yr) >> download_15min_open_data(yr)
    
    @task.bash(env = BASH_ENV)
    def download_locations_open_data()->str:
        return '''/usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c \
                "SELECT location_dir_id, location_name, direction, linear_name_full, side_street,
                    longitude, latitude, centreline_id, bin_size, latest_calibration_study,
                    first_active, last_active, date_decommissioned, technology
                FROM open_data.cycling_permanent_counts_locations
                ORDER BY location_dir_id;" \
                --csv -o "$EXPORT_PATH/cycling_permanent_counts_locations.csv"'''
                
    @task.bash(env = BASH_ENV)
    def download_locations_open_data_geojson()->str:
        return '''/usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c \
    "SELECT featurecollection FROM open_data.cycling_permanent_counts_locations_geojson;" \
    --tuples-only -o "$EXPORT_PATH/cycling_permanent_counts_locations.geojson"'''
            
    @task.bash()
    def output_readme()->str:
        source='/home/airflow/bdit_data-sources/_/volumes/open_data/sql/cycling_permanent_counts_readme.md'
        dest='cycling_permanent_counts_readme.pdf'
        return f'''pandoc -V geometry:margin=1in -o "{EXPORT_PATH}/{dest}" "{source}"'''
    
    @task(
        retries=0,
        trigger_rule='all_success',
        doc_md="""A status message to report DAG success."""
    )
    def status_message(ds = None, **context):
        mnth = ds_format(ds, '%Y-%m-%d', '%Y-%m-01')
        send_slack_msg(
            context=context,
            msg=f"Ecocounter :open_data_to: DAG ran successfully for {mnth} :white_check_mark:. "
            f"Remember to `cp {EXPORT_PATH}/* /data/open_data/permanent-bike-counters` as bigdata.",
            use_proxy=True
        )
    
    yrs = get_years()
    (
        check_data_availability >>
        reminder_message() >>
        wait_till_10th >> 
        update_locations >> [
            insert_and_download_data.expand(yr = yrs),
            download_locations_open_data(),
            download_locations_open_data_geojson(),
            output_readme()
        ] >>
        status_message()
    )
        
ecocounter_open_data_dag()
