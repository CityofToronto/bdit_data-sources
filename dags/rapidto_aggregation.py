"""
airflow_rapidto_aggregation

A data aggregation workflow that aggregates travel time data for
RapidTO, inserting hourly travel times for each report_id
into rapidto.report_daily_hr_summary daily. 
This DAG is schedule to run only when pull_here DAG
finished running. 
"""
import os
import sys
import logging
import pendulum
from datetime import timedelta

from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
except ImportError:
    raise ImportError("Cannot import DAG helper functions.")

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

DAG_NAME = 'rapidto_aggregation'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ['Unknown'])

default_args = {'owner': ','.join(DAG_OWNERS),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2023, 4, 1, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

@dag(
    dag_id = DAG_NAME, 
    default_args=default_args, 
    schedule=None, #gets triggered by here dag
    catchup=False,
    tags=["HERE"]
)
def rapidto_aggregation():
    ## Postgres Tasks
    # Task to aggregate segment level tt daily
    aggregate_daily = SQLExecuteQueryOperator(
        sql='''SELECT rapidto.generate_daily_hr_agg('{{ macros.ds_add(ds, -1) }}')''',
        task_id='aggregate_daily',
        conn_id='natalie',
        autocommit=True,
        retries = 0
    )

    #wait_for_here >> 
    aggregate_daily

rapidto_aggregation()