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
from airflow.decorators import dag, task, task_group
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from dateutil.relativedelta import relativedelta
from airflow.models import Variable


from datetime import datetime, timedelta
import logging
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

bt_bot = PostgresHook('bt_bot')

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
except:
    raise ImportError("Cannot import DAG helper functions.")

try:
    sys.path.append('/data/airflow/data_scripts/bluetooth/sql/') 
    from bt_eoy_create_tables import replace_bt_trigger
except Exception as exc:
    err_msg = "Error importing functions for end of year Bluetooth maintenance \n" + str(exc)
    raise ImportError(err_msg)

SLACK_CONN_ID = 'slack_data_pipeline'
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
    tags=["partition_create"],
    catchup=False,
    doc_md=__doc__
    ) 
def eoy_create_table_dag():
    @task_group
    def yearly_task():
        """Task group to create yearly tables and triggers."""
        bt_replace_trigger = PythonOperator(
                        task_id='bt_replace_trigger',
                        python_callable = replace_bt_trigger,
                        op_kwargs = {'pg_hook': bt_bot,
                                      'dt': '{{ ds }}'})
        @task
        def insert_holidays(**kwargs):
            dt = kwargs["ds"]
            next_year = datetime.strptime(dt, "%Y-%m-%d") + relativedelta(years=1)
            holidays_year = holidays.CA(prov='ON', years=int(next_year.year))
            ref_bot = PostgresHook('ref_bot')
            with ref_bot.get_conn() as con, con.cursor() as cur:
                for dt, name in holidays_year.items():
                    name = name.replace('Observed', 'obs')
                    cur.execute('INSERT INTO ref.holiday VALUES (%s, %s)', (dt, name))

        here_create_tables = PostgresOperator(
                        task_id='here_create_tables',
                        sql="SELECT here.create_yearly_tables('{{ macros.ds_format(data_interval_end | ds, '%Y-%m-%d', '%Y') }}')",
                        postgres_conn_id='here_bot',
                        autocommit=True)
        
        bt_create_tables = PostgresOperator(
                        task_id='bluetooth_create_tables',
                        sql="SELECT bluetooth.create_obs_tables('{{ macros.ds_format(data_interval_end | ds, '%Y-%m-%d', '%Y') }}')",
                        postgres_conn_id='bt_bot',
                        autocommit=True)
        
        congestion_create_table = PostgresOperator(
                        task_id='congestion_create_table',
                        sql="SELECT congestion.create_yearly_tables('{{ macros.ds_format(data_interval_end | ds, '%Y-%m-%d', '%Y') }}')",
                        postgres_conn_id='congestion_bot',
                        autocommit=True)
        
        bt_replace_trigger
        insert_holidays()
        here_create_tables
        bt_create_tables
        congestion_create_table

    success_alert = SlackWebhookOperator(
        task_id="success_alert",
        slack_webhook_conn_id = SLACK_CONN_ID,
        message='''All End of Year tables have been successfully created, 
                please checkout the tables on the database and make sure 
                they have been properly created. ''',
        username="airflow"
    )

    yearly_task() >> success_alert

eoy_create_table_dag()