#!/data/airflow/airflow_venv/bin/python3
# -*- coding: utf-8 -*-
# noqa: D415
r"""### The Daily Collision Replicator DAG

This DAG runs daily to update the following collisions tables from the tables
staged in the database by the MOVE's BDITTO_COLLISIONS_REPLICATOR DAG:

1\. ACC

2\. events

3\. involved

4\. events_centreline
"""
import os
import sys
from datetime import timedelta
import pendulum
from psycopg2 import sql
# pylint: disable=import-error
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

# import custom operators and helper functions
repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
# pylint: disable=wrong-import-position
from dags.dag_functions import task_fail_slack_alert
# pylint: enable=import-error
# pylint: enable=wrong-import-position

DAG_NAME = 'collisions_replicator'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ['Unknown'])

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 10, 31, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert,
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    catchup=False,
    max_active_runs=5,
    schedule_interval="0 7 * * *",
    doc_md=__doc__
)
def collisions_replicator():
    """The main function of the collisions DAG."""
    tables = [
        ("collisions_staging.ACC", "collisions.ACC"),
        ("collisions_staging.events", "collisions.events"),
        ("collisions_staging.involved", "collisions.involved"),
        ("collisions_staging.events_centreline", "collisions.events_centreline")
    ]
    # connection to PostgreSQL
    @task()
    def copy_table(src:str, dst:str) -> None:
        """Copies ``src`` table into ``dst`` after truncating it.

        Args:
            src: Source table in format ``schema.table``
            dst: Destination table in format ``schema.table``
        """
        con = PostgresHook("collisions_bot").get_conn()
        src_schema, src_table = src.split(".")
        dst_schema, dst_table = dst.split(".")
        truncate_query = sql.SQL(
            "TRUNCATE {}.{}"
            ).format(
                sql.Identifier(dst_schema), sql.Identifier(dst_table)
            )
        insert_query = sql.SQL(
            "INSERT INTO {}.{} SELECT * FROM {}.{}"
            ).format(
                sql.Identifier(dst_schema), sql.Identifier(dst_table),
                sql.Identifier(src_schema), sql.Identifier(src_table)
            )
        with con, con.cursor() as cur:
            cur.execute(truncate_query)
            cur.execute(insert_query)

    for src, dst in tables:
        copy_table.override(task_id=dst.split(".")[1])(src, dst)

collisions_replicator()
