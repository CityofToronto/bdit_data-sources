r"""### Monthly ecocounter Open Data DAG
Pipeline to run monthly ecocounter aggregations for Open Data.
"""
import sys
import os
from datetime import timedelta
import logging
import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable 
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
    'on_failure_callback': task_fail_slack_alert
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
        doc_md="Wait for last day of month to run before running monthly DAG."
        timeout=86400, #one day
        #when this DAG runs on Monday (day 7 - at the end of it's week long schedule interval),
        #it should check for the Sunday (day 6) _pull DAG, which gets executed on the Monday.
        execution_delta=timedelta(days=-1, hours=-1) #pull_ecocounter scheduled at '0 10 * * *'
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
        mnth = ds_format(ds, '%Y-%m-%d', '%Y-%m-01')
        slack_ids = Variable.get("slack_member_id", deserialize_json=True)
        list_names = " ".join([slack_ids.get(name, name) for name in DAG_OWNERS])

        send_slack_msg(
            context=context,
            msg=f"{list_names} Remember to check Ecocounter Open Data for {mnth} and label any sites pending validation in anomalous_ranges. :meow_detective: :open_data_to:"
        )

    wait_till_10th = DateTimeSensor(
        task_id="wait_till_10th",
        timeout=10*86400,
        mode="reschedule",
        poke_interval=3600*24,
        target_time="{{ execution_date.replace(day=10) }}",
    )
    wait_till_10th.doc_md = """
    Wait until the 10th day of the month to export data. Alternatively mark task as success to proceed immediately.
    """

    #these tasks will dump data from database to morbius.
    #refresh_monthly_open_data = PostgresOperator(
    #    task_id='refresh_monthly_open_data',
    #    sql="SELECT gwolofs.insert_ecocounter_open_data_monthly_summary('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date)",
    #    postgres_conn_id='ecocounter_bot',
    #    autocommit=True
    #)
    #
    #refresh_15min_open_data = PostgresOperator(
    #    task_id='refresh_15min_open_data',
    #    sql="SELECT gwolofs.insert_ecocounter_15min_open_data('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date)",
    #    postgres_conn_id='ecocounter_bot',
    #    autocommit=True
    #)

    @task(
        retries=0,
        trigger_rule='all_success',
        doc_md="""A status message to report DAG success."""
    )
    def status_message(ds = None, **context):
        mnth = ds_format(ds, '%Y-%m-%d', '%Y-%m-01')
        send_slack_msg(
            context=context,
            msg=f":meow_ecocounter: :open_data_to: DAG ran successfully for {mnth} :white_check_mark:"
        )

    (
        t_upstream_done >>
        check_data_availability >>
        reminder_message() >>
        wait_till_10th >>
        #[refresh_monthly_open_data, refresh_15min_open_data] >> 
        status_message()
    )

ecocounter_open_data_dag()