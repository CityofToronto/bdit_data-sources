#!/data/airflow/airflow_venv/bin/python3
# -*- coding: utf-8 -*-
# noqa: D415
r"""### The Daily counts Replicator DAG

This DAG runs daily to copy MOVE's counts tables from the ``move_staging``
schema, which is updated by the MOVE's ``bigdata_replicator`` DAG, to the
``traffic`` schema. This DAG runs only when it is triggered by the MOVE's
DAG.
"""
import os
import sys
from datetime import timedelta
from functools import partial
import pendulum
# pylint: disable=import-error
from airflow.decorators import dag
from airflow.models import Variable

# import custom operators and helper functions
repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
# pylint: disable=wrong-import-position
from dags.dag_functions import task_fail_slack_alert
# pylint: enable=import-error

DAG_NAME = "counts_replicator"
DAG_OWNERS = Variable.get("dag_owners", deserialize_json=True).get(DAG_NAME, ["Unknown"])

default_args = {
    "owner": ",".join(DAG_OWNERS),
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 10, 31, tz="America/Toronto"),
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=60),
    # "on_failure_callback": task_fail_slack_alert,
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    catchup=False,
    max_active_runs=5,
    schedule_interval=None,
    doc_md=__doc__,
    tags=["counts"]
)
def counts_replicator():
    """The main function of the counts DAG."""
    from dags.common_tasks import (
        wait_for_external_trigger, get_list_of_tables, copy_table
    )

    tables = get_list_of_tables("counts_tables")
    wait_for_external_trigger() >> tables
    copy_table.override(task_id="copy_tables").partial(conn_id="traffic_bot").expand(table=tables)

counts_replicator()
