r"""### Monthly Miovision Open Data DAG
Pipeline to run monthly Miovision aggregations for Open Data.
"""
import sys
import os

from airflow.decorators import dag, task
from datetime import timedelta
from airflow.models import Variable 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.macros import ds_format

import logging
import pendulum

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert, send_slack_msg
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
except:
    raise ImportError("Cannot import DAG helper functions.")

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

DAG_NAME = 'miovision_open_data'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past':False,
    #set earlier start_date + catchup when ready?
    'start_date': pendulum.datetime(2024, 1, 1, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule='0 10 3 * *', # 10am, 3rd day of each month
    catchup=True,
    max_active_runs=1,
    tags=["miovision", "open_data"],
    doc_md=__doc__
)
def miovision_open_data_dag():

    #considered whether it should have an external task sensor
    #for the first of the month. Decided it should run later
    #to give time for anomalous_range updates if any.

    check_data_availability = SQLCheckOperatorWithReturnValue(
        task_id="check_data_availability",
        sql="""WITH daily_volumes AS (
            SELECT dt::date, COALESCE(SUM(daily_volume), 0) AS daily_volume
            FROM generate_series('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date,
                                 '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date + '1 month'::interval - '1 day'::interval,
                                 '1 day'::interval) AS dates(dt)
            LEFT JOIN miovision_api.volumes_daily_unfiltered USING (dt)
            GROUP BY dt
            ORDER BY dt
        )

        SELECT NOT(COUNT(*) > 0), 'Missing dates: ' || string_agg(dt::text, ', ')
        FROM daily_volumes
        WHERE daily_volume = 0""",
        conn_id="miovision_api_bot"
    )

    refresh_monthly_open_data = PostgresOperator(
        task_id='refresh_monthly_open_data',
        sql="SELECT gwolofs.insert_miovision_open_data_monthly_summary('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date)",
        postgres_conn_id='miovision_api_bot',
        autocommit=True
    )
    
    refresh_15min_open_data = PostgresOperator(
        task_id='refresh_15min_open_data',
        sql="SELECT gwolofs.insert_miovision_15min_open_data('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date)",
        postgres_conn_id='miovision_api_bot',
        autocommit=True
    )

    @task(
        retries=0,
        trigger_rule='all_success',
        doc_md="""A status message to report DAG success."""
    )
    def status_message(ds = None, **context):
        mnth = ds_format(ds, '%Y-%m-%d', '%Y-%m-01')
        send_slack_msg(
            context=context,
            msg=f":meow_miovision: :open_data_to: DAG ran successfully for {mnth} :white_check_mark:"
        )

    (
        check_data_availability >>
        [refresh_monthly_open_data, refresh_15min_open_data] >> 
        status_message()
    )

miovision_open_data_dag()
