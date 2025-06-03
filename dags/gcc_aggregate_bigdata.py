import os
import sys
import logging
import pendulum
from datetime import timedelta

from airflow.sdk import dag, Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert
except ImportError:
    raise ImportError("Cannot import DAG helper functions.")

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

DAG_NAME = 'gcc_aggregate_bigdata'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ['Unknown'])

execute_sql = partial(SQLExecuteQueryOperator, conn_id='gcc_bot_bigdata', autocommit=True, retries = 0)

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past':False,
    'start_date': pendulum.datetime(2025, 6, 1, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule=None, #triggered by gcc_layers_pull_bigdata
    catchup=False,
)
def gcc_aggregate_bigdata():
    
    ## Tasks ##
    # SQLSensor to test if all of last week's data is in the database
    # date set as ds - 9 because this task runs on a wednesday and the input should be last monday
    ## Postgres Tasks ##
    # Task to aggregate bluetooth data weekly
    upstream_agg = execute_sql(
        sql="""
            REFRESH MATERIALIZED VIEW gis_core.centreline_latest;
            REFRESH MATERIALIZED VIEW gis_core.centreline_latest_all_feature;
        """,
        task_id='upstream_agg'
    )
    
    #all these depend on centreline_latest:
    downstream_agg = execute_sql(
        sql="""
            REFRESH MATERIALIZED VIEW gis_core.centreline_intersection_point_latest;
            REFRESH MATERIALIZED VIEW gis_core.intersection_latest;
            REFRESH MATERIALIZED VIEW gis_core.centreline_leg_directions;
            REFRESH MATERIALIZED VIEW traffic.svc_centreline_directions;
        """,
        task_id='downstream_agg'
    )

    ## Flow ##
    # Once check tasks are marked as successful, aggregation task can be scheduled
    upstream_agg >> downstream_agg

gcc_aggregate_bigdata()