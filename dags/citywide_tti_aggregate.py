import sys
import os

from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.macros import ds_add, ds_format

import logging
import pendulum
try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert
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

default_args = {'owner': ','.join(DAG_OWNERS),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2024, 5, 16, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

@dag(
    DAG_NAME, 
    default_args=default_args, 
    schedule=None, # gets triggered by HERE dag
    doc_md = doc_md,
    tags=["HERE", "aggregation"],
    catchup=False
)


def citywide_tti_aggregate():

# Task to aggregate citwyide tti

    aggregate_daily = SQLExecuteQueryOperator(sql="SELECT covid.generate_citywide_tti( '{{macros.ds_add(ds, -1)}}' )",
                                       task_id='aggregate_daily',
                                       conn_id='congestion_bot',
                                       autocommit=True,
                                       retries = 0
                                       )
    aggregate_daily

citywide_tti_aggregate()