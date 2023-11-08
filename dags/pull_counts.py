#!/data/airflow/airflow_venv/bin/python3
# -*- coding: utf-8 -*-
# noqa: D415
r"""### The Daily counts Replicator DAG

This DAG runs daily to update the following counts tables from the tables
staged in the database by the MOVE's ``bigdata_replicator`` DAG:

1\. arteries_centreline

2\. arteries_counts_groups

3\. arteries_groups

4\. arterydata

5\. atr_metadata

6\. atr_study_data

7\. cnt_det

8\. count_info

9\. counts_multiday_runs

10\. oracle_cnt_det

11\. studies
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
    "on_failure_callback": task_fail_slack_alert,
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
    from dags.common_tasks import wait_for_external_trigger, copy_table

    tables = [
        ("move_staging.arteries_centreline", "traffic.arteries_centreline"),
        ("move_staging.arteries_counts_groups", "traffic.arteries_counts_groups"),
        ("move_staging.arteries_groups", "traffic.arteries_groups"),
        ("move_staging.arterydata", "traffic.arterydata"),
        ("move_staging.atr_metadata", "traffic.atr_metadata"),
        ("move_staging.atr_study_data", "traffic.atr_study_data"),
        ("move_staging.cnt_det", "traffic.cnt_det"),
        ("move_staging.count_info", "traffic.count_info"),
        ("move_staging.counts_multiday_runs", "traffic.counts_multiday_runs"),
        ("move_staging.oracle_cnt_det", "traffic.oracle_cnt_det"),
        ("move_staging.studies", "traffic.studies")
    ]
    
    wait_for_external_trigger() >> copy_table.partial(
        conn_id="traffic_bot").expand(table=tables)

counts_replicator()
