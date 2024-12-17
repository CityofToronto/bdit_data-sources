# The official new GCC puller DAG file
import sys
import os
from functools import partial

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
    from gis.gccview.gcc_puller_functions import get_layer
except:
    raise ImportError("Cannot import DAG helper functions.")


# the DAG runs at 7 am on the first day of January, April, July, and October
def create_gcc_puller_dag(dag_id, default_args, name, conn_id):
    @dag(
        dag_id=dag_id,
        default_args=default_args,
        catchup=False,
        tags=['gcc', name],
        schedule='0 7 1 */3 *' #'@quarterly'
    )
    def gcc_layers_dag():
        
        @task()
        def get_layers(name):
            tables = Variable.get('gcc_layers', deserialize_json=True)
            return tables[name]
                    
        @task(map_index_template="{{ table_name }}")
        def pull_layer(layer, conn_id):
            #name mapped task
            context = get_current_context()
            context["table_name"] = layer[0]
            #get db connection
            conn = PostgresHook(conn_id).get_conn()
            
            #pull and insert layer
            get_layer(
                mapserver_n = layer[1].get("mapserver"),
                layer_id = layer[1].get("layer_id"),
                schema_name = layer[1].get("schema_name"),
                is_audited = layer[1].get("is_audited"),
                primary_key = layer[1].get("pk"),
                con = conn
            )

            #refresh mat views as necessary
            agg_sql = layer[1].get("agg")
            if agg_sql is not None:
                with conn.cursor() as cur:
                    cur.execute(agg_sql)

        layers = get_layers(name)
        pull_layer.partial(conn_id = conn_id).expand(layer = layers)

    generated_dag = gcc_layers_dag()

    return generated_dag

#get puller details from airflow variable
DAGS = Variable.get('gcc_dags', deserialize_json=True)

#identify the appropriate pullers based on deployment
dep = os.environ.get("DEPLOYMENT", "PROD")
filtered_dags = [
    key for key, facts in DAGS.items() if dep in facts['deployments']
]

for item in filtered_dags:
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

    dag_name = f"{DAG_NAME}_{item}"
    globals()[dag_name] = (
        create_gcc_puller_dag(
            dag_id=dag_name,
            default_args=DEFAULT_ARGS,
            name=item,
            conn_id=DAGS[item]['conn'],
        )
    )
