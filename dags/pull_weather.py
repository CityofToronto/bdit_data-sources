"""
Pipeline to pull weather prediction data from Envrionment Canada and upsert into weather.prediction_daily table.
Then, 
A Slack notification is raised when the airflow process fails.
"""
import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.latest_only_operator import LatestOnlyOperator

#connection credentials
cred = PostgresHook("weather_bot")

#import python scripts
try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0,os.path.join(repo_path,'weather'))
    from prediction_import import prediction_upsert
    from mrc_import import historical_upsert
except:
    raise ImportError("script import failed")


SLACK_CONN_ID = 'slack_data_pipeline'
dag_config = Variable.get('slack_member_id', deserialize_json=True)
list_names = dag_config['raphael'] + ' ' + dag_config['islam'] + ' ' + dag_config['natalie'] 


def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password

    if context.get('task_instance').task_id == 'pull_prediction':
        task_msg = """:cat_shock: The Task {task} in Pull Weather dag failed, 
			{slack_name} please check.""".format(
            task=context.get('task_instance').task_id, slack_name = list_names,)
    
    else:
        task_msg = """ :eyes: The Task {task} in Pull Weather dag failed, 
			{slack_name} please check.""".format(
            task=context.get('task_instance').task_id, slack_name = list_names,)    
        
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



#DAG
 
default_args = {
    'owner':'rdumas',
    'depends_on_past':False,
    'start_date': datetime(2022, 11, 8),
    'email': ['raphael.dumas@toronto.ca'],
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

dag = DAG('pull_weather', default_args=default_args, schedule_interval='30 23 * * *', catchup=False)

#=======================================#
#no backfill
no_backfill = LatestOnlyOperator(task_id="no_backfill", dag=dag)

#dag tasks

PULL_PREDICTION = PythonOperator(
    task_id = 'pull_prediction',
    python_callable = prediction_upsert,
    dag=dag,
    op_args=[cred]
)

PULL_HISTORICAL = PythonOperator(
    task_id = 'pull_historical',
    python_callable = historical_upsert,
    dag=dag,
    op_args=[cred, '{{ds}}']
)

no_backfill >> PULL_PREDICTION
no_backfill >> PULL_HISTORICAL