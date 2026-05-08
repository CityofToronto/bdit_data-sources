import os
import sys
import logging
from pendulum import duration, datetime

from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert
    from bdit_dag_utils.utils.common_tasks import check_1st_of_month
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
    'retries': 1,
    'retry_delay': duration(hours=1),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    DAG_NAME,
    default_args=default_args,
    schedule='0 16 * * *', # 4pm daily
    #schedule=None, #triggered daily by here_dynamic_binning_agg_hm
    template_searchpath=os.path.join(repo_path, 'congestion'),
    doc_md=doc_md,
    tags=["HERE", "aggregation", "congestion", "TTI"],
    max_active_runs=1,
    catchup=True
)

#to add: catchup, one task at a time, depends on past.

def tti_dag():
    
    check_missing_dates = SQLCheckOperatorWithReturnValue(
        sql="SELECT _check, _summary FROM here_agg.check_month_present(('{{ ds }}'::date - interval '1 month')::date)",
        task_id="check_missing_dates",
        conn_id=CONN_ID,
        retries = 0,
        pre_execute=check_1st_of_month, #agg previous month, only if 1st of month
    )
    
    aggregate_segment_lookback = SQLExecuteQueryOperator(
        sql="SELECT here_agg.agg_segment_6month_lookback('{{ ds }}'::date);", #no need to subtract month here.
        task_id="aggregate_segment_lookback",
        conn_id=CONN_ID,
        retries = 1,
        trigger_rule="none_failed",
        pre_execute=check_1st_of_month, #agg previous month, only if 1st of month
    )
    
    aggregate_hrly = SQLExecuteQueryOperator(
        sql="SELECT here_agg.hourly_avg_tt_agg('{{ ds }}'::date);",
        task_id="aggregate_hrly",
        conn_id=CONN_ID,
        retries = 1,
        trigger_rule="none_failed",
    )
    
    aggregate_area_tti = SQLExecuteQueryOperator(
        sql="SELECT here_agg.area_tti_agg('{{ ds }}'::date);",
        task_id="aggregate_area_tti",
        conn_id=CONN_ID,
        retries = 1,
    )
    
    check_missing_dates >> [aggregate_segment_lookback] >> aggregate_hrly >> aggregate_area_tti

tti_dag()