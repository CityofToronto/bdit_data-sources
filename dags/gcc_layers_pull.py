# The official new GCC puller DAG file
import sys
import os
from functools import partial

import pendulum
from psycopg2 import sql
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import get_current_context

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
    from dags.common_tasks import get_variable
    from gis.gccview.gcc_puller_functions import get_layer
except:
    raise ImportError("Cannot import DAG helper functions.")

DAG_NAME = 'gcc_pull_layers'

DAG_OWNERS  = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

DEFAULT_ARGS = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 11, 3, tz="America/Toronto"),
    'email_on_failure': False, 
    'retries': 0,
    'on_failure_callback': partial(task_fail_slack_alert, use_proxy=True)
}

# the DAG runs at 7 am on the first day of January, April, July, and October
@dag(
    dag_id = DAG_NAME,
    catchup=False,
    default_args=DEFAULT_ARGS,
    schedule='0 7 1 */3 *' #'@quarterly'
)
def gcc_layers_dag():

    @task()
    def get_pullers():
        dep = os.environ.get("DEPLOYMENT", "PROD")
        pullers = Variable.get('gcc_pullers', deserialize_json=True)

        #return the appropriate pullers
        return [(puller, facts['conn']) for puller, facts in pullers.items() if dep in facts['deployments']]

    @task_group()
    def pull_and_agg_layers(puller_details):
        @task()
        def get_conn_id(puller_details) -> str:
            return puller_details[1]

        @task()
        def get_layers(puller: str):
            tables = Variable.get('gcc_layers', deserialize_json=True)
            return tables[puller]
        
        @task(map_index_template="{{ table_name }}")
        def pull_layer(layer, conn_id):
            #name mapped task
            context = get_current_context()
            context["table_name"] = tables[tbl_index]
            conn = PostgresHook(conn_id).get_conn()
            get_layer(layer, layer.items(), conn)
        
        #if layer in ['centreline', 'intersection']:
        #    @task
        #    def agg_layer():
        #        sql_refresh_mat_view = sql.SQL("SELECT {function_name}()").format(
        #            function_name=sql.Identifier('gis', f'refresh_mat_view_{layer}_version_date')
        #        )
        #        with con.cursor() as cur:
        #            cur.execute(sql_refresh_mat_view)

        pull_layer.partial(conn_id = get_conn_id(puller_details)).expand(layer = get_layers(puller_details))() #>> agg_layer()

    for puller in get_pullers():
        pull_and_agg_layers(puller_details = puller)

gcc_layers_dag()