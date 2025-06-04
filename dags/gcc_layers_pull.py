# The official new GCC puller DAG file
import sys
import os
from functools import partial
import pendulum

from airflow.sdk import dag, task, task_group, get_current_context, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.param import Param
from airflow.exceptions import AirflowFailException

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert
    from gis.gccview.gcc_puller_functions import (
        get_layer, mapserver_name, get_src_row_count, get_dest_row_count
    )
except:
    raise ImportError("Cannot import DAG helper functions.")


# the DAG runs at 7 am on the first day of January, April, July, and October
def create_gcc_puller_dag(dag_id, default_args, name, conn_id, aggs_to_trigger):
    @dag(
        dag_id=dag_id,
        default_args=default_args,
        catchup=False,
        tags=["bdit_data-sources", "gcc", name, "quarterly"],
        params={
            "layer_name": Param(
                default='',
                type=["null", "string"],
                title="Layer name.",
                description="Layer name for custom pull. Example: city_ward",
            ),
            "is_audited": Param(
                default=False,
                type=["null", "boolean"],
                title="is_audited",
                description="Is the layer audited?",
            ),
            "include_additional_feature": Param(
                default=False,
                type=["null", "boolean"],
                title="Include additional feature",
                description="Flag to include additional feature type",
            ),
            "layer_id": Param(
                default=0,
                type=["null", "integer"],
                title="layer_id.",
                description="layer_id for custom pull. Example: 0",
            ),
            "mapserver": Param(
                default=0,
                type=["null", "integer"],
                title="mapserver",
                description="mapserver for custom pull. Example: 0",
            ),
            "pk": Param(
                default='',
                type=["null", "string"],
                title="Primary Key.",
                description="(Optional) primary key for custom pull. Example: area_id",
                examples=['area_id', 'objectid'],
            ),
            "schema_name": Param(
                default='',
                type=["null", "string"],
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
            
        @task_group
        def pull_and_check(conn_id, layer):
            
            @task(map_index_template="{{ table_name }}")
            def pull_layer(conn_id, layer):
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
                    msg = f"`{schema}.{table_name}` - Source count: `{src_count}`, Bigdata count: `{dest_count}`"
                    context.get("task_instance").xcom_push(key="extra_msg", value=msg)
                    raise AirflowFailException(msg)
                return True
                
            pull = pull_layer(conn_id, layer)
            check = compare_row_counts(conn_id, layer)
            pull >> check

        # Create a task group for triggering the aggs
        @task_group(group_id='trigger_agg_tasks')
        def trigger_aggs(conn_id, downstream_aggs):
            # Define SQLExecuteQueryOperator for each agg to trigger

            prev_task=None
            for task_id in downstream_aggs:
                sql_operator = SQLExecuteQueryOperator(
                    task_id=task_id,
                    sql=downstream_aggs[task_id],
                    conn_id=conn_id,
                    autocommit=True,
                    retries = 0
                )
                # Create sequential dependency chain
                if prev_task:
                    prev_task >> sql_operator
                prev_task = sql_operator
                
        layers = get_layers(name)
        [
            pull_and_check.partial(conn_id = conn_id).expand(layer = layers) >>
            trigger_aggs(conn_id, downstream_aggs)
        ]
        
    generated_dag = gcc_layers_dag()

    return generated_dag

#A dictionary of GCC DAGs to create using dynamic DAG generation.
#Dictionary keys correspond to keys in `gcc_layers` variable.
#"conn": Postgres connection ID.
#"deployment": A list of the deployments to deploy this DAG on.
#"downstream_aggs": An ordered list of sqls to run after layers are finished pulling.
DAGS = {
    "bigdata": {
        "conn": "gcc_bot_bigdata",
        "deployments": ["DEV"],
        "downstream_aggs": {
            "centreline_latest": "REFRESH MATERIALIZED VIEW gis_core.centreline_latest;",
            "centreline_latest_all_feature": "REFRESH MATERIALIZED VIEW gis_core.centreline_latest_all_feature;",
            "centreline_intersection_point_latest": "REFRESH MATERIALIZED VIEW gis_core.centreline_intersection_point_latest;",
            "intersection_latest": "REFRESH MATERIALIZED VIEW gis_core.intersection_latest;",
            "centreline_leg_directions": "REFRESH MATERIALIZED VIEW gis_core.centreline_leg_directions;",
            "svc_centreline_directions": "REFRESH MATERIALIZED VIEW traffic.svc_centreline_directions;"
        }
    },
    "ptc": {
        "conn": "gcc_bot",
        "deployments": ["DEV", "PROD"]
    },
    "sirius": {
        "conn": "gcc_bot_sirius",
        "deployments": ["DEV"],
        "downstream_aggs": {
            "centreline_latest": "REFRESH MATERIALIZED VIEW gis.centreline_latest;"
        }
    }
}

#identify the appropriate pullers based on deployment
dep = os.environ.get("DEPLOYMENT", "PROD")
filtered_dags = [
    key for key, facts in DAGS.items() if dep in facts['deployments']
]

for item in filtered_dags:
    DAG_NAME = 'gcc_pull_layers'
    DAG_OWNERS  = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])
    downstream_aggs = DAGS[item].get('downstream_aggs', [])
    
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
            aggs_to_trigger=downstream_aggs
        )
    )
