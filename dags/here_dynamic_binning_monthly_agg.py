import os
import sys
import logging
from pendulum import duration, datetime

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    'retries': 1,
    'retry_delay': duration(hours=1)
    #'on_failure_callback': task_fail_slack_alert
}

@dag(
    DAG_NAME,
    default_args=default_args,
    schedule='0 16 1 * *', # 4pm, first day of month
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
        retries = 0
    )
    
    aggregate_monthly = SQLExecuteQueryOperator(
        sql=[
            "DELETE FROM gwolofs.congestion_segments_monthy_summary WHERE mnth = '{{ ds }}'",
            "SELECT gwolofs.congestion_segment_monthly_agg('{{ ds }}')"
        ],
        task_id='aggregate_monthly',
        conn_id='congestion_bot',
        autocommit=True,
        retries = 1
    )
    
    create_groups = SQLExecuteQueryOperator(
        sql="segment_grouping.sql",
        task_id="create_segment_groups",
        #TODO: update sql to work for different map versions
        start_date=datetime(2025, 4, 1, tz="America/Toronto"),
        conn_id='congestion_bot',
        retries = 0,
        params={"max_group_size": 100}
    )
    
    delete_data = SQLExecuteQueryOperator(
        sql="DELETE FROM gwolofs.congestion_segments_monthly_bootstrap WHERE mnth = '{{ ds }}' AND n_resamples = 300",
        task_id="delete_bootstrap_results",
        conn_id='congestion_bot',
        retries=0
    )
    
    @task
    def expand_groups(**context):
        return context["ti"].xcom_pull(task_ids="create_segment_groups")[0][0]
    
    @task(retries=0, max_active_tis_per_dag=1)
    def bootstrap_agg(segments, ds):
        print(f"segments: {segments}")
        postgres_cred = PostgresHook("congestion_bot")
        query="""SELECT *
            FROM UNNEST(%s::bigint[]) AS unnested(segment_id),
            LATERAL (
                SELECT gwolofs.congestion_segment_bootstrap(
                                mnth := %s::date,
                                segment_id := segment_id,
                                n_resamples := 300)
            ) AS lat"""
        with postgres_cred.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (segments, ds))
                conn.commit()
    
    expand = expand_groups()
    
    check_missing_dates >> aggregate_monthly >> create_groups >> delete_data
    delete_data >> expand
    bootstrap_agg.expand(segments=expand)

here_dynamic_binning_monthly_agg()