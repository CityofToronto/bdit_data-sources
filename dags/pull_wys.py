"""
Pipeline to pull Watch Your Speed sign data data and put them into the wys.raw_data table using Python Operator.
A Slack notification is raised when the airflow process fails.
"""
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.postgres_hook import PostgresHook


try:
    sys.path.append('/etc/airflow/data_scripts/wys/api/python/')
    from wys_api import api_main
except:
    raise ImportError("Cannot import functions to pull watch your speed data")


SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: WYS Api Pull Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow'
    )
    return failed_alert.execute(context=context)


wys_postgres = PostgresHook("wys_bot")

connection = BaseHook.get_connection('wys_api_key')
api_key = connection.password

default_args = {'owner':'rdumas',
                'depends_on_past':False,
                'start_date': datetime(2020, 1, 7),
                'email': ['raphael.dumas@toronto.ca'],
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }

dag = DAG('pull_wys',default_args=default_args, schedule_interval='0 11 * * *')
# Run at 8 AM local time every monday

with wys_postgres.get_conn() as conn:
    t1 = PythonOperator(
            task_id = 'pull_wys',
            python_callable=api_main, 
            dag=dag,
            op_kwargs={'conn':conn, 'api_key':api_key}
            )
