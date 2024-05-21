import sys
import os

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from airflow.decorators import dag, task

from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2 import connect, Error
import logging
import pendulum
try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
except:
    raise ImportError("Cannot import slack alert functions")
    
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

doc_md = """

### The Daily Citywide TTI Aggregation DAG

This DAG runs daily to aggregate citywide TTI after the pull_here dag finishes.
Slack notifications is raised when the airflow process fails.

"""
DAG_NAME = 'citywide_tti_aggregate'

DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"]) 

# Slack alert
SLACK_CONN_ID = 'slack_data_pipeline'

default_args = {'owner':'natalie',
                'depends_on_past':False,
                'start_date': pendulum.datetime(2024, 5, 16, tz="America/Toronto"),
                'email': ['natalie.chan@toronto.ca'],
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

@dag(
    DAG_NAME, 
    default_args=default_args, 
    schedule_interval='30 16 * * * ', # same as pull_here task 
    doc_md = doc_md,
    tags=["HERE"],
    catchup=False
)

## Tasks ##
## ExternalTaskSensor to wait for pull_here
def citywide_tti_aggregate():
    
    wait_for_here = ExternalTaskSensor(task_id='wait_for_here',
                                       external_dag_id='pull_here',
                                       external_task_id='pull_here',
                                       start_date=datetime(2024, 5, 16)
                                       )

## Postgres Tasks
# Task to aggregate citwyide tti
    aggregate_daily = PostgresOperator(sql='''SELECT covid.generate_citywide_tti('{{ yesterday_ds }}')''',
                                       task_id='aggregate_daily',
                                       postgres_conn_id='natalie',
                                       autocommit=True,
                                       retries = 0
                                       )

    wait_for_here >> aggregate_daily

citywide_tti_aggregate()