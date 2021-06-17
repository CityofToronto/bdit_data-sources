from airflow import DAG
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.sensors import ExternalTaskSensor

from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2 import connect, Error
import logging
from calendar import monthrange

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# Slack alert
SLACK_CONN_ID = 'slack_natalie'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    task_msg = 'WYS vol_chart did not get refreshed'   
    slack_msg = task_msg + """(<{log_url}|log>)""".format(
            log_url=context.get('task_instance').log_url,)
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        )
    return failed_alert.execute(context=context)

def task_succeed_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    task_msg = 'WYS vol_chart finished refreshing'   
    slack_msg = task_msg + """(<{log_url}|log>)""".format(
            log_url=context.get('task_instance').log_url,)
    success_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        )
    return success_alert.execute(context=context)    

default_args = {'owner':'natalie',
                'depends_on_past':False,
                'start_date': datetime(2020, 7, 1),
                'email': ['natalie.chan@toronto.ca'],
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert,
                'on_success_callback': task_succeed_slack_alert
                }

dag = DAG('refresh_tps_email', 
          default_args=default_args, 
          schedule_interval='0 15 * * *', # same as pull_wys task 
          catchup=False,
)

wait_for_wys = ExternalTaskSensor(
    task_id='wait_for_wys',
    external_dag_id='pull_wys',
    external_task_id='pull_wys',
    start_date=datetime(2020, 4, 1),
)

refresh_tps_views = PostgresOperator(sql='''refresh materialized view covid.wys_volchart with data''',
                            task_id='refresh_tps_views',
                            postgres_conn_id='natalie',
                            autocommit=True,
                            retries = 0,
                            dag=dag)

wait_for_wys >> refresh_tps_views