"""
Pipeline to pull miovision daily data and put them into postgres tables using Bash Operator.
Slack notifications is raised when the airflow process fails.
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.postgres_operator import PostgresOperator


SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed. 
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
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        # proxy='http://137.15.73.132:8080'
        )
    return failed_alert.execute(context=context)

default_args = {'owner':'rdumas',
                'depends_on_past':False,
                'start_date': datetime(2019, 11, 22),
                'email': ['raphael.dumas@toronto.ca'],
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }

dag = DAG('pull_miovision',default_args=default_args, schedule_interval='0 8 * * *')
# Add 5 hours to make up for the different time zone then another 3 hours to ensure that the data are at least 2 hours old

t1 = BashOperator(
        task_id = 'pull_miovision',
        bash_command = '/etc/airflow/data_scripts/.venv/bin/python3 /etc/airflow/data_scripts/volumes/miovision/api/intersection_tmc.py run-api --path /etc/airflow/data_scripts/volumes/miovision/api/config.cfg --dupes', 
        retries = 0,
        dag=dag)

refresh_views = PostgresOperator(sql='SELECT miovision_api.refresh_matview()',
                            task_id='refresh_view',
                            postgres_conn_id='miovision_api_bot',
                            autocommit=True,
                            retries = 0,
                            dag=dag)

t1 >> refresh_views