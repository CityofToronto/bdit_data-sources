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
from airflow.sdk import dag

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert
    from bdit_dag_utils.utils.custom_operators import SQLCheckOperatorWithReturnValue
    from bdit_dag_utils.utils.common_tasks import check_jan_1st
except:
    raise ImportError("Cannot import slack alert functions")
    
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

doc_md = "This DAG is running off the `1132-here-aggregation-proposal` branch to test dynamic binning aggregation."
DAG_NAME = 'here_dynamic_binning_agg'
DAG_OWNERS = "Gabe"
CONN_ID = "congestion_bot"

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past':False,
    'start_date': datetime(2019, 1, 1, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 1,
    'retry_delay': duration(minutes=5),
    'on_failure_callback': task_fail_slack_alert
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
    create_partitions = SQLExecuteQueryOperator(
            task_id='create_partitions',
            pre_execute=check_jan_1st,
            sql=#partition by year
                """SELECT gwolofs.partition_yyyy(
                    base_table := ''congestion_raw_segments'',
                    year_ := '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int,
                    partition_owner := 'gwolofs',
                    schema_ := 'gwolofs'
                );""",
            conn_id=CONN_ID,
            autocommit=True
    )
    
    check_not_empty = SQLCheckOperatorWithReturnValue(
        task_id="check_not_empty",
        sql="SELECT COUNT(*), COUNT(*) FROM here.ta_path WHERE dt = '{{ ds }}'",
        conn_id=CONN_ID,
        retries=1,
        retry_delay=duration(days=1),
        trigger_rule='none_failed'
    )
    
    delete_daily = SQLExecuteQueryOperator(
        sql="DELETE FROM gwolofs.congestion_raw_segments WHERE dt = '{{ ds }}'",
        task_id='delete_daily',
        conn_id=CONN_ID,
        autocommit=True,
        retries = 2
    )
    
    aggregate_daily = SQLExecuteQueryOperator(
        sql="SELECT gwolofs.congestion_network_segment_agg('{{ ds }}'::date);",
        task_id='aggregate_daily',
        conn_id=CONN_ID,
        autocommit=True,
        retries = 2,
        hook_params={"options": "-c statement_timeout=10800000ms"} #3 hours
    )
    
    create_partitions >> check_not_empty >> delete_daily >> aggregate_daily

here_dynamic_binning_agg()