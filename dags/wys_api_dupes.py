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
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.sql import SQLCheckOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

# Credentials
from airflow.hooks.postgres_hook import PostgresHook
wys_postgres = PostgresHook("wys_bot")
wys_uri = wys_postgres.get_uri()

SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed. <UHJA7GHQV> <U1XGLNWG2>
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
# minutes past each hour | Hours (0-23) | Days of the month (1-31) | Months (1-12) | Days of the week (0-7, Sunday represented as either/both 0 and 7)

DUPES_DAG = DAG(
    'dupes_dag',
    default_args=DEFAULT_ARGS, 
    schedule_interval='0 5 * * *', # Run at 5am local time each day 
    catchup=False
)

#CHECK_DUPES = BashOperator(
#    task_id='wys_api_count_dupes',
#    bash_command = '''/home/cnangini/PROJECTS/bdit_data-sources/wys/api/python/wys_api_count_dupes.sh ''', # SPACE !!!!
#    env={'wys_uri':wys_uri},
#    retries=0,
#    dag=DUPES_DAG
#)

#CHECK_DUPES = PostgresOperator(
#    sql='''select count(*) from wys.mobile_sign_installations_dupes''',
#    task_id='wys_api_count_dupes',
#    postgres_conn_id='wys_bot',
#    autocommit=True,
#    retries = 0,
#    dag=DUPES_DAG
#)

# operator = SQLCheckOperator(
#     sql="SELECT COUNT(*) FROM wys.mobile_sign_installations_dupes"
# )

# https://stackoverflow.com/questions/66005381/error-in-airflow-sqlcheckoperator-attributeerror-nonetype-object-has-no-att
CHECK_DUPES = SQLCheckOperator(
    task_id='wys_api_count_dupes',
    sql="SELECT COUNT(*) FROM wys.mobile_sign_installations_dupes",
    postgres_conn_id='wys_bot',
    #use_legacy_sql=False,
    dag=DUPES_DAG
)
#operator = SQLCheckOperator(
#     sql="SELECT COUNT(*) FROM wys.mobile_sign_installations_dupes"
#)

# To run:
# airflow test dupes_dag wys_api_count_dupes 29/08/2019

