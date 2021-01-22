"""
Pipeline to populate bluetooth reader_status_history table every day 
using a Bash Operator.
"""
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.postgres_hook import PostgresHook

SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Date Update Failed on bluetooth routes table. 
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

    
default_args = {'owner':'mohan',
                'depends_on_past':False,
                'start_date': datetime(2021, 1, 21),
                'email': ['mohanraj.adhikari@toronto.ca'],
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }

dag = DAG('update_bt_status_history',default_args=default_args, schedule_interval='0 12 * * *')

## Postgres update bluetooth reader_status_history DAG
# Task to update the reader_status_history table daily
bt_status_history = PostgresOperator(sql='''SELECT * from mohan.reader_status_history(current_date)''',
                            task_id='bt_status_history',
                            postgres_conn_id='mohan',
                            autocommit=True,
                            retries = 0,
                            dag=dag)