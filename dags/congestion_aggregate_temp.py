import sys
import os
import logging
import pendulum
from datetime import timedelta

from airflow.sdk import dag
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert
    from dags.dag_owners import owners
except:
    raise ImportError("Cannot import slack alert functions")
    
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

doc_md = """

### The Daily TTI Aggregation DAG

This DAG runs daily to aggregate congestion segments daily (23_4) TEMPORARILY  after the pull_here dag finishes.
Slack notifications is raised when the airflow process fails.

"""
DAG_NAME = 'congestion_aggregate_temp'
DAG_OWNERS = owners.get(DAG_NAME, ["Unknown"]) 

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
    tags=["HERE", "temp"],
    catchup=False
)

def congestion_aggregate_temp():
    aggregate_temp = SQLExecuteQueryOperator(
        sql="SELECT congestion.generate_network_daily_temp_23_4( '{{macros.ds_add(ds, -1)}}' )",
        task_id='aggregate_temp',
        conn_id='congestion_bot',
        autocommit=True,
        retries = 0
    )
    
    aggregate_temp

congestion_aggregate_temp()
