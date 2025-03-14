# The official new GCC puller DAG file
import sys
import os
from functools import partial

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context
from airflow.models.param import Param
from airflow.exceptions import AirflowFailException

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
    from gis.gccview.gcc_puller_functions import (
        get_layer, mapserver_name, get_src_row_count, get_dest_row_count
    )
except:
    raise ImportError("Cannot import DAG helper functions.")


# the DAG runs at 7 am on the first day of January, April, July, and October
def create_gcc_puller_dag(dag_id, default_args, name, conn_id):
    @dag(
        dag_id=dag_id,
        default_args=default_args,
        catchup=False,
        tags=["bdit_data-sources", "gcc", name, "quarterly"],
        params={
            "layer_name": Param(
                default='',
                type="string",
                title="Layer name.",
                description="Layer name for custom pull. Example: city_ward",
            ),
            "is_audited": Param(
                default=False,
                type="boolean",
                title="is_audited",
                description="Is the layer audited?",
            ),
            "include_additional_feature": Param(
                default=False,
                type="boolean",
                title="Include additional feature",
                description="Flag to include additional feature type",
            ),
            "layer_id": Param(
                default=0,
                type="integer",
                title="layer_id.",
                description="layer_id for custom pull. Example: 0",
            ),
            "mapserver": Param(
                default=0,
                type="integer",
                title="mapserver",
                description="mapserver for custom pull. Example: 0",
            ),
            "pk": Param(
                default='',
                type="string",
                title="Primary Key.",
                description="(Optional) primary key for custom pull. Example: area_id",
                examples=['area_id', 'objectid'],
            ),
            "schema_name": Param(
                default='',
                type="string",
                title="schema_name.",
                description="schema_name for custom pull",
                examples=['gis', 'gis_core'],
            )
        },
        schedule='0 7 1 */3 *' #'@quarterly'
    )
    def gcc_layers_dag():
        
        @task()
        def get_layers(name, **context):
            layer_name = context["params"]["layer_name"]
            if layer_name == "":
                tables = Variable.get('gcc_layers', deserialize_json=True)
                return tables[name]
            #if layer_name param was used to trigger, return only the custom layer.
            layer = dict({
                'is_audited': context["params"]["is_audited"],
                'layer_id': context["params"]["layer_id"],
                'mapserver': context["params"]["mapserver"],
                'pk': context["params"]["pk"],
                'schema_name': context["params"]["schema_name"],
                'include_additional_feature':context["params"]["include_additional_feature"],
            })
            return dict({layer_name:layer})
                
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
                include_additional_feature = layer[1].get("include_additional_feature"),
                primary_key = layer[1].get("pk"),
                con = conn
            )
            #refresh mat views as necessary
            agg_sql = layer[1].get("agg")
            if agg_sql is not None:
                with conn.cursor() as cur:
                    cur.execute(agg_sql)

        @task(map_index_template="{{ table_name }}")
        def compare_row_counts(conn_id, layer, ds):
            
            mapserver = mapserver_name(layer[1].get("mapserver"))
            schema = layer[1].get("schema_name")
            is_audited = layer[1].get("is_audited")
            include_additional_feature = layer[1].get("include_additional_feature")
            layer_id = layer[1].get("layer_id")
            table_name = layer[0]

            #name mapped task
            context = get_current_context()
            context["table_name"] = table_name

            src_count = get_src_row_count(
                    mapserver = mapserver,
                    layer_id = layer_id,
                    include_additional_feature = include_additional_feature
                )
            conn = PostgresHook(conn_id).get_conn()
            dest_count = get_dest_row_count(conn, schema, table_name, is_audited, ds)
            if src_count != dest_count:
                raise AirflowFailException(f"src_count: {src_count}, dest_count: {dest_count}")
            return True

        layers = get_layers(name)
        pull = pull_layer.partial(conn_id = conn_id).expand(layer = layers)
        check = compare_row_counts.partial(conn_id = conn_id).expand(layer = layers)
        pull >> check
        
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
