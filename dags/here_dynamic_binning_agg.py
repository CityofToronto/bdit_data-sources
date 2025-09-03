'''
To trigger for past date (range) use CLI:
for i in {0..5}; do 
    end_date=$(date -I -d "2023-11-02 +$i days")
    airflow dags trigger -e "${end_date}" here_dynamic_binning_agg
done

or trigger just one day: airflow dags trigger -e 2023-11-02 here_dynamic_binning_agg
`airflow dags backfill ...` doesn't work because there are no scheduled run dates in that range.
'''

import sys
import os
import logging
from pendulum import duration, datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.decorators import dag, task

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
DAG_NAME = 'here_dynamic_binning_agg'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"]) 

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past':False,
    'start_date': datetime(2019, 1, 1, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 1,
    'retry_delay': duration(minutes=5),
    #'on_failure_callback': task_fail_slack_alert
}

@dag(
    DAG_NAME, 
    default_args=default_args, 
    schedule=None, # triggered by `pull_here_path` DAG
    doc_md = doc_md,
    tags=["HERE", "aggregation"],
    max_active_runs=1,
    catchup=False
)

#to add: catchup, one task at a time, depends on past.

def here_dynamic_binning_agg():
    check_not_empty = SQLCheckOperatorWithReturnValue(
        task_id="check_not_empty",
        sql="SELECT COUNT(*), COUNT(*) FROM here.ta_path WHERE dt = '{{ ds }}'",
        conn_id="congestion_bot",
        retries=1,
        retry_delay=duration(days=1)
    )
    
    aggregate_daily = SQLExecuteQueryOperator(
        sql=["DELETE FROM gwolofs.congestion_raw_segments WHERE dt = '{{ ds }}'",
             "SELECT gwolofs.congestion_network_segment_agg('{{ ds }}'::date);"],
        task_id='aggregate_daily',
        conn_id='congestion_bot',
        autocommit=True,
        retries = 2,
        hook_params={"options": "-c statement_timeout=10800000ms"} #3 hours
    )
    
    check_not_empty >> aggregate_daily

here_dynamic_binning_agg()