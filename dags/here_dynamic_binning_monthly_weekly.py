import os
import sys
import logging
from pendulum import duration, datetime

from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowFailException
from airflow.task.trigger_rule import TriggerRule

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

doc_md = "This DAG is running off the `1132-here-aggregation-proposal` branch to test dynamic binning aggregation."
DAG_NAME = 'here_dynamic_binning_weekly_agg'
DAG_OWNERS = owners.get(DAG_NAME, ['Unknown'])
CONN_ID = "congestion_bot"

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 1, tz="America/Toronto"),
    'retries': 1,
    'retry_delay': duration(hours=1),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    DAG_NAME,
    default_args=default_args,
    schedule='0 14 * * MON', # 2pm Monday
    template_searchpath=os.path.join(repo_path,'here/traffic/sql/dynamic_bins'),
    doc_md=doc_md,
    tags=["HERE", "aggregation"],
    max_active_runs=1,
    catchup=False
)

#to add: catchup, one task at a time, depends on past.

def here_dynamic_binning_weekly_agg():
    
    check_missing_dates = SQLCheckOperatorWithReturnValue(
        sql="""SELECT _check, _summary FROM here_agg.check_data_availability(
            '{{ ds }}'::date - 2,
            '{{ ds }}'::date + 5 --exclusive end date
        );""",
        task_id="check_missing_dates",
        conn_id=CONN_ID,
        retries = 0
    )
        
    create_groups = SQLExecuteQueryOperator(
        sql="""SELECT segments FROM here_agg.segment_grouping(
            '{{ ds }}'::date - 2,
            '{{ ds }}'::date + 5 --exclusive end date
        );""",
        task_id="create_segment_groups",
        conn_id=CONN_ID,
        retries = 0,
        params={"max_group_size": 100}
    )
    
    delete_data = SQLExecuteQueryOperator(
        sql="DELETE FROM here_agg.segments_bootstrap_weekly WHERE week_start = '{{ ds }}'::date - 2;",
        task_id="delete_bootstrap_results",
        conn_id=CONN_ID,
        retries=0
    )
    
    @task
    def expand_groups(**context):
        return context["ti"].xcom_pull(task_ids="create_segment_groups")[0][0]
    
    @task(
        retries=0,
        max_active_tis_per_dag=1,
        on_failure_callback=None #downstream task to notify
    )
    def bootstrap_agg(segments, ds):
        batch_size = 10
        postgres_cred = PostgresHook(CONN_ID)
        query = """SELECT here_agg.weekly_bootstrap(%s::date - 2, %s::bigint[]);"""
        segments_list = sorted(int(x) for x in segments.strip("{}").split(","))
        
        with postgres_cred.get_conn() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(segments_list), batch_size):
                    batch = segments_list[i:i + batch_size]
                    print(f"Processing batch {i // batch_size + 1}: {batch}")
                    cur.execute(query, (ds, batch))
                    conn.commit()
    
    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def notify_on_upstream_failure():
        """Task to notify on upstream mapped task failure."""
        raise AirflowFailException("An upstream mapped task failed.")
    
    expand = expand_groups()
    
    check_missing_dates >> create_groups >> delete_data
    delete_data >> expand
    bootstrap_agg.expand(segments=expand) >> notify_on_upstream_failure()

here_dynamic_binning_weekly_agg()