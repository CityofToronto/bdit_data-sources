import os
import sys
import logging
from pendulum import duration, datetime

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert
    from bdit_dag_utils.utils.custom_operators import SQLCheckOperatorWithReturnValue
    from dags.dag_owners import owners
except:
    raise ImportError("Cannot import slack alert functions")
    
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

doc_md = "This DAG is running off the `congestion_tti` branch to test TTI aggregation."
DAG_NAME = 'tti_dag'
DAG_OWNERS = owners.get(DAG_NAME, ['Unknown'])
CONN_ID = "congestion_bot"

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1, tz="America/Toronto"),
    #temporary end date while figuring out congestion network versions
    'end_date': datetime(2026, 1, 1, tz="America/Toronto"),
    'retries': 1,
    'retry_delay': duration(hours=1),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    DAG_NAME,
    default_args=default_args,
    schedule='0 16 3 * *', # 4pm, 3rd day of month
    template_searchpath=os.path.join(repo_path, 'congestion'),
    doc_md=doc_md,
    tags=["HERE", "aggregation"],
    max_active_runs=1,
    catchup=True
)

#to add: catchup, one task at a time, depends on past.

def tti_dag():
    
    check_missing_dates = SQLCheckOperatorWithReturnValue(
        sql="select-check_missing_days.sql",
        task_id="check_missing_dates",
        conn_id=CONN_ID,
        retries = 0
    )
    
    aggregate_vkt = SQLExecuteQueryOperator(
        sql="SELECT here_agg.monthly_link_vkt_agg('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date);",
        task_id="aggregate_vkt",
        conn_id=CONN_ID,
        retries = 1
    )
    
    aggregate_overnight = SQLExecuteQueryOperator(
        sql="SELECT here_agg.agg_overnight_tt('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date);",
        task_id="aggregate_overnight",
        conn_id=CONN_ID,
        retries = 1
    )
    
    aggregate_hrly = SQLExecuteQueryOperator(
        sql="SELECT here_agg.hourly_avg_tt_agg('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date);",
        task_id="aggregate_hrly",
        conn_id=CONN_ID,
        retries = 1
    )
    
    aggregate_area_tti = SQLExecuteQueryOperator(
        sql="SELECT here_agg.area_tti_agg('{{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-01') }}'::date);",
        task_id="aggregate_area_tti",
        conn_id=CONN_ID,
        retries = 1
    )
    
    check_missing_dates >> [aggregate_vkt, aggregate_overnight, aggregate_hrly] >> aggregate_area_tti

tti_dag()