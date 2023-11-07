#!/data/airflow/airflow_venv/bin/python3
# -*- coding: utf-8 -*-
# noqa: D415
r"""### The Daily Collision Replicator DAG

This DAG runs daily to update the following collisions tables from the tables
staged in the database by the MOVE's ``bigdata_replicator`` DAG:

1\. acc

2\. events

3\. involved

4\. events_centreline
"""
import os
import sys
from datetime import timedelta
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

DAG_NAME = "collisions_replicator"
DAG_OWNERS = Variable.get("dag_owners", deserialize_json=True).get(DAG_NAME, ["Unknown"])

default_args = {
    "owner": ",".join(DAG_OWNERS),
    "depends_on_past": False,
    "start_date": pendulum.datetime(2023, 10, 31, tz="America/Toronto"),
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=60),
    "on_failure_callback": task_fail_slack_alert,
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    catchup=False,
    max_active_runs=5,
    schedule_interval=None,
    doc_md=__doc__,
    tags=["collisions"]
)
def collisions_replicator():
    """The main function of the collisions DAG."""
    from dags.common_tasks import wait_for_external_trigger, copy_table

    tables = [
        ("move_staging.acc", "collisions.acc"),
        ("move_staging.events", "collisions.events"),
        ("move_staging.involved", "collisions.involved"),
        ("move_staging.events_centreline", "collisions.events_centreline")
    ]
    
    wait_for_external_trigger() >> copy_table.expand(table=tables)

collisions_replicator()
