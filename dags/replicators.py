#!/data/airflow/airflow_venv/bin/python3
# -*- coding: utf-8 -*-
# noqa: D415
import os
import sys
from functools import partial
from pendulum import datetime, duration
# pylint: disable=import-error
from airflow.sdk import dag, task, task_group, get_current_context
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

# import custom operators and helper functions
repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_owners import owners
# pylint: disable=wrong-import-position
from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, send_slack_msg, check_not_empty
# pylint: enable=import-error

def create_replicator_dag(dag_id, short_name, tables_var, conn, doc_md, default_args): 
    @dag(
        dag_id=dag_id,
        default_args=default_args,
        catchup=False,
        max_active_runs=5,
        max_active_tasks=5,
        schedule=None, #triggered externally
        doc_md=doc_md,
        tags=[short_name, "replicator"]
    )
    def replicator_DAG():
        f"""The main function of the {short_name} DAG."""
        from bdit_dag_utils.utils.common_tasks import (
            wait_for_external_trigger, get_variable, copy_table
        )

        # Returns a list of source and destination tables stored in the given
        # Airflow variable.
        tables = get_variable.override(task_id="get_list_of_tables")(tables_var)
        
        @task_group()
        def copy_tables_with_checks(conn_id, tables):

            @task(on_failure_callback = None, map_index_template="{{ dest_table_name }}")
            def check_tbl_not_empty(conn_id, tables, tbl_index, **context):
                #name mapped task
                context = get_current_context()
                context["dest_table_name"] = tables[tbl_index]
                
                #check table is not empty
                table = tables[tbl_index]
                check_not_empty(context, conn_id, table)

            [
                check_tbl_not_empty.override(task_id="check_source_not_empty")(conn_id, tables, 0) >>
                copy_table.override(task_id="copy_tables", on_failure_callback = None)(conn_id, tables) >>
                check_tbl_not_empty.override(task_id="check_dest_not_empty")(conn_id, tables, 1)
            ]

        @task(
            retries=0,
            trigger_rule='all_done',
            doc_md="""A status message to report DAG success OR any failures from the `copy_tables` task."""
        )
        def status_message(tables, **context):
            ti = context["ti"]
            failures = []
            #iterate through mapped tasks to find any failure messages
            for m_i in range(0, len(tables)):
                extra_msg = partial(ti.xcom_pull, key="extra_msg", map_indexes=m_i)
                failure_0 = extra_msg(task_ids="copy_tables_with_checks.check_source_not_empty")
                failure_1 = extra_msg(task_ids="copy_tables_with_checks.copy_tables")
                failure_2 = extra_msg(task_ids="copy_tables_with_checks.check_dest_not_empty")
                #pick the first non empty failure message from each mapped index.
                failure_msg = failure_0 or failure_1 or failure_2
                if failure_msg is not None:
                    failures.append(failure_msg)
            if failures == []:
                send_slack_msg(
                    context=context,
                    msg=f"`{dag_id}` DAG succeeded :white_check_mark:"
                )
            else: #add details of failures to task_fail_slack_alert
                failure_extra_msg = ['One or more tables failed to copy:', failures]
                context.get("task_instance").xcom_push(key="extra_msg", value=failure_extra_msg)
                raise AirflowFailException('One or more tables failed to copy.')
            
        # Waits for an external trigger
        [
            wait_for_external_trigger() >>
            copy_tables_with_checks.partial(conn_id=conn).expand(tables=tables) >>
            status_message(tables=tables)
        ]

    generated_dag = replicator_DAG()

    return generated_dag

"""Dictionary of replicator DAGs to create

dag_name: dag_id
tables: name of Airflow variable to get list of (src/dest) tables to replicate
conn: name of Airflow connection of bot used for replication
"""
REPLICATORS =  {
    "counts": {
        "dag_name": "counts_replicator",
        "tables": "counts_tables",
        "conn": "traffic_bot"
    },
    "collisions": {
        "dag_name": "collisions_replicator",
        "tables": "collisions_tables",
        "conn": "collisions_bot"
    }
}

#generate replicator DAGs from dict
for replicator, dag_items in REPLICATORS.items():
    DAG_NAME = dag_items['dag_name']
    DAG_OWNERS = owners.get(DAG_NAME, ["Unknown"])

    default_args = {
        "owner": ",".join(DAG_OWNERS),
        "depends_on_past": False,
        "start_date": datetime(2023, 10, 31, tz="America/Toronto"),
        "email_on_failure": False,
        "retries": 3,
        "retry_delay": duration(minutes=60),
        "on_failure_callback": task_fail_slack_alert,
    }

    doc_md = f"""### The Daily {replicator} Replicator DAG

    This DAG runs daily to copy MOVE's {replicator} tables from the ``move_staging``
    schema, which is updated by the MOVE's ``bigdata_replicator`` DAG, to the
    ``{replicator}`` schema. This DAG runs only when it is triggered by the MOVE's
    DAG."""

    globals()[DAG_NAME] = (
        create_replicator_dag(
            dag_id=DAG_NAME,
            short_name=replicator,
            tables_var=dag_items['tables'],
            conn=dag_items['conn'],
            doc_md=doc_md,
            default_args=default_args
        )
    )