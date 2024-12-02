r"""### Yearly End of Year DAG
Pipeline to create yearly partitioned tables for the following table, once a year on Decemeber 1st: 
- here.ta
- bluetooth.observations
- congestion.network_segment_daily

Slack notifications is raised for both successful and failed airflow process.
"""
import sys
import os
import holidays
import pendulum
import logging
from datetime import timedelta

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert, send_slack_msg
    from bluetooth.sql.bt_eoy_create_tables import replace_bt_trigger
except:
    raise ImportError("Cannot import DAG helper functions.")

DAG_NAME = 'eoy_table_create'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

default_args = {'owner': ','.join(DAG_OWNERS), 
                'depends_on_past':False,
                'start_date': pendulum.datetime(2021, 12, 1, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule='0 0 1 12 *', # At 00:00 on 1st in December.
    tags=["partition_create", "yearly"],
    catchup=False,
    doc_md=__doc__
)
def eoy_create_table_dag():
    @task
    def yr(data_interval_end=None)->int:
        return data_interval_end.year + 1
         
    @task_group
    def yearly_task(YR=None):
        """Task group to create yearly tables and triggers."""
              
        @task
        def bt_replace_trigger(yr=None):
            bt_bot = PostgresHook('bt_bot')
            replace_bt_trigger(bt_bot, yr)

        @task
        def insert_holidays(yr=None):
            holidays_year = holidays.CA(prov='ON', years=int(yr))
            ref_bot = PostgresHook('ref_bot')
            with ref_bot.get_conn() as con, con.cursor() as cur:
                for dt, name in holidays_year.items():
                    name = name.replace('Observed', 'obs')
                    cur.execute('INSERT INTO ref.holiday VALUES (%s, %s)', (dt, name))

        here_create_tables = PostgresOperator(
                        task_id='here_create_tables',
                        sql="SELECT here.create_yearly_tables('{{ task_instance.xcom_pull('yr') }}')",
                        postgres_conn_id='here_bot',
                        autocommit=True)
        
        bt_create_tables = PostgresOperator(
                        task_id='bluetooth_create_tables',
                        sql="SELECT bluetooth.create_obs_tables('{{ task_instance.xcom_pull('yr') }}')",
                        postgres_conn_id='bt_bot',
                        autocommit=True)
        
        congestion_create_table = PostgresOperator(
                        task_id='congestion_create_table',
                        sql="SELECT congestion.create_yearly_tables('{{ task_instance.xcom_pull('yr') }}')",
                        postgres_conn_id='congestion_bot',
                        autocommit=True)
        
        bt_replace_trigger(yr=YR)
        insert_holidays(yr=YR)
        here_create_tables
        bt_create_tables
        congestion_create_table

    @task(trigger_rule='none_failed')
    def success_alert(**context):
        slack_ids = Variable.get("slack_member_id", deserialize_json=True)
        list_names = " ".join([slack_ids.get(name, name) for name in DAG_OWNERS])
        send_slack_msg(
            context=context,
            msg=f"{list_names} EOY DAG has run successfully. Please check out the tables on the database and make sure "
                "they have been properly created."
        )

    yearly_task(YR=yr()) >> success_alert()

eoy_create_table_dag()