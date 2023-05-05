"""
Pipeline for pulling traffic signal data from Oracle database in Traffic Control
group. A view called 'signals_cart' is already set up in the public schema of the
'traffic_signals' database in the local PostgreSQL server on the BDIT Linux box.
The view was created in pgAdmin from SIGNALVIEW Oracle tables using Foreign Data
Wrappers.
"""
from datetime import datetime
import os
import sys
import psycopg2
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable 

# ------------------------------------------------------------------------------
# Credentials
from airflow.hooks.postgres_hook import PostgresHook
vz_cred = PostgresHook("vzbigdata") # name of Conn Id defined in UI
vz_pg_uri = vz_cred.get_uri()

# ------------------------------------------------------------------------------
# Slack notification
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

dag_name = 'traffic_dag'

dag_owners = Variable.get('dag_owners', deserialize_json=True)
slack_ids = Variable.get('slack_member_id', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

list_names = []
for name in names:
    list_names.append(slack_ids.get(name, '@Unknown Slack ID')) #find slack ids w/default = Unkown

SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed / Tâche échouée. LOCALHOST AIFRLOW
            {slack_name} please check.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
            slack_name = ' '.join(list_names),
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
        proxy='http://137.15.73.132:8080'
        )
    return failed_alert.execute(context=context)

# ------------------------------------------------------------------------------
AIRFLOW_DAGS = os.path.dirname(os.path.realpath(__file__))
AIRFLOW_ROOT = os.path.dirname(AIRFLOW_DAGS)
AIRFLOW_TASKS = os.path.join(AIRFLOW_ROOT, 'assets/traffic_signals/airflow/tasks')

DEFAULT_ARGS = {
    'email_on_failure': False,
    'email_on_retry': False,
    'owner': names, 
    'start_date': datetime(2019, 7, 9), # YYYY, MM, DD
    'task_concurrency': 1,
    'on_failure_callback': task_fail_slack_alert
}

# ------------------------------------------------------------------------------
TRAFFIC_DAG = DAG(
    dag_id = dag_name,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    template_searchpath=[os.path.join(AIRFLOW_ROOT, 'assets/traffic_signals/airflow/tasks')],
    schedule_interval='0 4 * * 1-5')
    # minutes past each hour | Hours (0-23) | Days of the month (1-31) | Months (1-12) | Days of the week (0-7, Sunday represented as either/both 0 and 7)

COPY_VIEW = BashOperator(
    task_id='copy_signalscart',
    bash_command="/copy_signalscart.sh",
    env={'vz_pg_uri':vz_pg_uri},
    retries=0,
    dag=TRAFFIC_DAG
)

# To run:
# airflow test traffic_dag copy_signalscart 29/08/2019
