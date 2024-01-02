r"""### Yearly End of Year DAG
Pipeline to create yearly partitioned tables for the following table, once a year on Decemeber 1st: 
- here.ta
- bluetooth.observations
- congestion.network_segment_daily

Slack notifications is raised for both successful and failed airflow process.
"""
import sys
import os
import pendulum

from airflow import DAG
from datetime import datetime, timedelta
import logging
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator #get rid of after
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
from airflow.decorators import dag, task, task_group
import holidays

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
                'start_date': pendulum.datetime(2022, 12, 1, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }
def insert_holidays(dt):
    next_year = datetime.strptime(dt, "%Y-%m-%d") + relativedelta(years=1)
    holidays_year = holidays.CA(prov='ON', years=int(next_year.year))
    ref_bot = PostgresHook('ref_bot')
    with ref_bot.get_conn() as con, con.cursor() as cur:
        for dt, name in holidays_year.items():
            name = name.replace('Observed', 'obs')
            cur.execute('INSERT INTO ref.holiday VALUES (%s, %s)', (dt, name))

@dag(dag_id=DAG_NAME,
     default_args=default_args,
     schedule='@yearly', #9:05 on December 1st of every year
     tags=["partition_create"],
     doc_md=__doc__) 

def eoy_create_table_dag():
    @task_group()
    def yearly_task():
        bt_replace_trigger = PythonOperator(task_id='bt_replace_trigger',
                                            python_callable = replace_bt_trigger,
                                            dag = dag,
                                            op_kwargs = {'pg_hook': bt_bot,
                                                        'dt': '{{ ds }}'})

        insert_holidays = PythonOperator(task_id='insert_holidays',
                                        python_callable = insert_holidays,
                                        dag = dag,
                                        op_args = ['{{ ds }}'])
        
        here_create_tables = PostgresOperator(task_id='here_create_tables',
                                            sql="SELECT here.create_yearly_tables('{{ macros.ds_format(next_ds, '%Y-%m-%d', '%Y') }}')",
                                            postgres_conn_id='here_bot',
                                            autocommit=True)
        
        bt_create_tables = PostgresOperator(
                        task_id='bluetooth_create_tables',
                        sql="SELECT bluetooth.create_obs_tables('{{ macros.ds_format(next_ds, '%Y-%m-%d', '%Y') }}')",
                        postgres_conn_id='bt_bot',
                        autocommit=True
                    )
        congestion_create_table = PostgresOperator(
                        task_id='congestion_create_table',
                        sql="SELECT congestion.create_yearly_tables('{{ macros.ds_format(next_ds, '%Y-%m-%d', '%Y') }}')",
                        postgres_conn_id='congestion_bot',
                        autocommit=True
                    )
    @task
    def success_text():
        print('successful')

    yearly_task() >> success_text()

eoy_create_table_dag()