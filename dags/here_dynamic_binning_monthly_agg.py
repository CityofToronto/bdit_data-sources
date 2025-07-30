import os
import sys
import logging
from datetime import timedelta
from pendulum import duration, datetime

from airflow.models import Variable
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
except:
    raise ImportError("Cannot import slack alert functions")
    
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

doc_md = "This DAG is running off the `1132-here-aggregation-proposal` branch to test dynamic binning aggregation."
DAG_NAME = 'here_dynamic_binning_monthly_agg'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"]) 

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past':False,
    'start_date': datetime(2019, 1, 1, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 1,
    'retry_delay': duration(hours=1),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    DAG_NAME,
    default_args=default_args,
    schedule='* 10 1 * *', # 10am, first day of month
    template_searchpath=os.path.join(repo_path,'here/traffic/sql/dynamic_bins'),
    doc_md = doc_md,
    tags=["HERE", "aggregation"],
    max_active_runs=1,
    catchup=True
)

#to add: catchup, one task at a time, depends on past.

def here_dynamic_binning_monthly_agg():
    
    check_missing_dates = SQLCheckOperatorWithReturnValue(
        sql="select-check_missing_days.sql",
        task_id="check_missing_dates",
        conn_id='congestion_bot',
        retries = 1,
        execution_timeout=timedelta(minutes=10)
    )
    
    aggregate_monthly = SQLExecuteQueryOperator(
        sql=[
            "DELETE FROM gwolofs.congestion_segments_monthy_summary WHERE mnth = '{{ ds }}'",
            "SELECT gwolofs.congestion_segment_monthly_agg('{{ ds }}')"
        ],
        task_id='aggregate_monthly',
        conn_id='congestion_bot',
        autocommit=True,
        retries = 1,
        execution_timeout=timedelta(hours=1)
    )
    check_missing_dates >> aggregate_monthly

here_dynamic_binning_monthly_agg()