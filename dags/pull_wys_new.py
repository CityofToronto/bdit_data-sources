"""
Pipeline to pull Watch Your Speed sign data data and put them into the wys.raw_data table using Python Operator.
A Slack notification is raised when the airflow process fails.
"""
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.check_operator import CheckOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.postgres_hook import PostgresHook

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from googleapiclient.discovery import build
from dateutil.relativedelta import relativedelta

try:
    sys.path.append('/etc/airflow/data_scripts/wys/api/python/')
    from wys_api import api_main
    from wys_google_sheet import read_masterlist
except:
    raise ImportError("Cannot import functions to pull watch your speed data")


SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password

    if context.get('task_instance').task_id == 't1':
        task_msg = """:cat_shocked: The Task {task} in Pull WYS dag failed, 
			<@UHJA7GHQV> please check.""".format(
            task=context.get('task_instance').task_id,)
    
    elif context.get('task_instance').task_id == 'check_wys_mobile_dupes_id':
        task_msg = """:cat_shocked: There are dupes in `wys.mobile_sign_installations_dupes`. 
			<@UHJA7GHQV> please check.""".format(
            task=context.get('task_instance').task_id,)
    
    # else other msg for task2
    else:
        task_msg = """:blob_fail: The Task {task} in Pull WYS dag failed, 
			<@UHJA7GHQV> please check.""".format(
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

#to get credentials to access google sheets
wys_api_hook = GoogleCloudBaseHook('vz_api_google')
cred = wys_api_hook._get_credentials()
service = build('sheets', 'v4', credentials=cred, cache_discovery=False)

#to connect to pgadmin bot
wys_postgres = PostgresHook("wys_bot")
connection = BaseHook.get_connection('wys_api_key')
api_key = connection.password

default_args = {'owner':'rdumas',
                'depends_on_past':False,
                'start_date': datetime(2020, 4, 1),
                'email': ['raphael.dumas@toronto.ca'],
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }

dag = DAG('pull_wys_new',default_args=default_args, schedule_interval='0 15 * * *')
# Run at 3 PM local time every day

with wys_postgres.get_conn() as con:
    t1 = PythonOperator(
            task_id = 'pull_wys',
            python_callable = api_main, 
            dag = dag,
            op_kwargs = {'conn':con, 
                        'start_date':'{{ ds }}', 
                        'end_date':'{{ ds }}', 
                        'api_key':api_key}
                        )
    
    t2 = PythonOperator(
            task_id = 'read_google_sheets',
            python_callable = read_masterlist,
            dag = dag,
            op_args = [con, service]
            )

# task2b will fail when there are a non-zero number of rows in the table
# and this will trigger a slack alert message
t2b = CheckOperator(
    task_id='check_wys_mobile_dupes_id',
    sql='''SELECT COUNT(1) = 0 FROM wys.mobile_sign_installations_dupes''',
    conn_id='wys_bot',
    dag=dag
)

# Dag order should be: t2 (read google sheets) then t2b (check for dupes)
# Task t1 (pull_wys) is independent of the other dags and so does not need to be ordered
t2 >> t2b


