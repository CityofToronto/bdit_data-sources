"""
Pipeline to pull CurbTO Intervetions daily and put them into postgres tables using Bash Operator.
Slack notifications is raised when the airflow process fails.
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator


SLACK_CONN_ID = 'slack_data_pipeline'
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
        username='airflow'
        )
    return failed_alert.execute(context=context)

default_args = {'owner':'natalie',
                'depends_on_past':False,
                'start_date': datetime(2020, 5, 26),
                'email': ['natalie.chan@toronto.ca'],
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }

dag = DAG('automate_interventions',default_args=default_args, schedule_interval='0 0 * * *')


t1 = BashOperator(
        task_id = 'pull_interventions',
        bash_command = '''/etc/airflow/data_scripts/.venv/bin/python3 /etc/airflow/data_scripts/gis/gccview/pull_interventions.py --mapserver='ActiveTO_and_CurbTO_View' --id=0''', 
        retries = 0,
        dag=dag)