#need new `vds` schema in bigdata
#need new `vds_bot` user in bigdata
#need 

from os import path
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from psycopg2 import sql
from psycopg2.extras import execute_values

try:
    #repo_path = path.abspath(path.dirname(path.dirname(path.realpath(__file__))))
    repo_path = '/home/gwolofs/bdit_data-sources'
    sys.path.insert(0,path.join(repo_path,'volumes/rescu/itscentral_pipeline'))
    from vds_functions import pull_raw_vdsdata, pull_raw_vdsvehicledata, summarize_into_v15, pull_detector_inventory
except:
    raise ImportError("Cannot import functions from volumes/rescu/itscentral_pipeline/vds_functions.py.")

dag_name = 'pull_vds'

# Get slack member ids
dag_owners = Variable.get('dag_owners', deserialize_json=True)
names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

SLACK_CONN_ID = 'slack_data_pipeline'
def task_fail_slack_alert(context):
    slack_ids = Variable.get('slack_member_id', deserialize_json=True)
    list_names = []
    for name in names:
        list_names.append(slack_ids.get(name, '@Unknown Slack ID')) #find slack ids w/default = Unkown

    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    # print this task_msg and tag these users
    task_msg = """The Task {task} failed. {slack_name} please check. """.format(
        task=context.get('task_instance').task_id, 
        slack_name = ' '.join(list_names),) 
    
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

#need to create these connections still
#CONNECT TO ITS_CENTRAL
itsc_bot = PostgresHook("itsc_postgres")

#CONNECT TO BIGDATA
#rescu_bot = PostgresHook("rescu_bot")
rescu_bot = PostgresHook("gwolofs_postgres")

default_args = {
    'owner': ','.join(names),
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    #'on_failure_callback': task_fail_slack_alert,
    'rds_conn': rescu_bot, #rds_conn = rescu_bot
    'itsc_conn': itsc_bot, #itsc_conn = itsc_bot
}

#start_date = '2023-06-01'
#end_date = '2023-06-01'

dag = DAG(dag_name, default_args=default_args, schedule_interval='0 4 * * *') #daily at 4am

#this one needs itsc and bdit pg connections
pull_raw_vdsdata_task = PythonOperator(
    task_id='pull_raw_vdsdata',
    python_callable=pull_raw_vdsdata,
    dag=dag,
    op_kwargs = {
            'start_date':'{{ ds }}', 
            'end_date':'{{ ds }}'
            } 
)

pull_raw_vdsvehicledata_task = PythonOperator(
    task_id='pull_raw_vdsvehicledata',
    python_callable=pull_raw_vdsvehicledata,
    dag=dag,
    op_kwargs = {
            'start_date':'{{ ds }}', 
            'end_date':'{{ ds }}'
            } 
)

summarize_data_task = PythonOperator(
    task_id='summarize_data',
    python_callable=summarize_into_v15,
    dag=dag,
    op_kwargs = {
            'start_date':'{{ ds }}', 
            'end_date':'{{ ds }}'
            } 
)

pull_detector_inventory_task = PythonOperator(
    task_id='pull_and_upsert_detector_inventory',
    python_callable=pull_detector_inventory,
    dag=dag
)

pull_raw_vdsdata_task >> summarize_data_task
pull_raw_vdsvehicledata_task
pull_detector_inventory_task