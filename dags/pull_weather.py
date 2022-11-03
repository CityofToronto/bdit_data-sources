"""
Pipeline to pull weather data from Envrionment Canada and upsert into weather.prediction_daily table.
A Slack notification is raised when the airflow process fails.
"""
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from dags.pull_wys import SLACK_CONN_ID 

SLACK_CONN_ID = 'slack_data_pipeline'
dag_config = Variable.get('slack_member_id', deserialize_json=True)
list_names = dag_config['raphael'] + ' ' + dag_config['islam'] + ' ' + dag_config['natalie'] 


def task_fail_slack_alert(context):
    return


#import python scripts
try:
    sys.path.append('script-path')
    from prediction_import import prediction_upsert
except:
    raise ImportError("script import failed")


#DAG

default_args = {
    'owner': 'radumas'
}

dag = DAG('pull_weather', default_args=default_args, schedule_interval='@daily', catchup=False)

#dag tasks
t1 = PythonOperator(
    task_id = 'pull_prediction',
    python_callable = prediction_upsert,
    dag=dag
)