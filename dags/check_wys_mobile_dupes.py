"""
Dag for counting the number of rows in table wys.mobile_sign_installations_dupes.
If a non-zero number of rows is found, the script sends an alert via a Slack
message.

"""

from datetime import datetime, timedelta
from airflow import DAG
import os
import sys
import psycopg2
from airflow import DAG
from airflow.operators.check_operator import CheckOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

# Slack error alert
SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password

    # print this task_msg and tag these users
    task_msg = """The Task {task} found dupes in `wys.mobile_sign_installations_dupes`.
        <@UHJA7GHQV> <@U1XGLNWG2> SVP jetez un oeil :eyes: :thanks_japanese: :meow_baguettehero:""".format(
        task=context.get('task_instance').task_id,)


    # this adds the error log url at the end of the msg
    slack_msg = task_msg + """ (<{log_url}|log>)""".format(
            log_url=context.get('task_instance').log_url,)
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        )
    return failed_alert.execute(context=context)

DEFAULT_ARGS = {
    'owner': 'cnangini',
    'depends_on_past' : False,
    'email': ['Cathy.Nangini@toronto.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime(2021, 4, 28), # YYYY, MM, DD
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

# ------------------------------------------------------------------------------
DUPES_DAG = DAG(
    'check_wys_mobile_dupes',
    default_args=DEFAULT_ARGS, 
    schedule_interval='0 5 * * *', # Run at 5am local time each day 
    catchup=False
)

# task1 will fail when there are a non-zero number of rows in the table
# and this will trigger a slack alert message
task1 = CheckOperator(
    task_id='check_wys_mobile_dupes',
    sql='''SELECT COUNT(1) = 0 FROM wys.mobile_sign_installations_dupes''',
    conn_id='wys_bot',
    dag=DUPES_DAG
)
