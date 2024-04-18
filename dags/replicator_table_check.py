"""Health checks for the MOVE -> bigdata replication process.
Checks for:
- `not_copied`: tables which are up to date in `move_staging` but not being copied downstream.
- `not_up_to_date`: tables which are being erroneously copied from `move_staging` without being up to date.
- `outdated_remove`: tables in `move_staging` which are outdated.
"""

#!/data/airflow/airflow_venv/bin/python3
# -*- coding: utf-8 -*-
# noqa: D415
import os
import sys
import pendulum
# pylint: disable=import-error
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

# import custom operators and helper functions
repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
# pylint: disable=wrong-import-position
from dags.dag_functions import task_fail_slack_alert, get_readme_docmd
from dags.common_tasks import wait_for_external_trigger
# pylint: enable=import-error

DAG_NAME = 'replicator_table_check'
DAG_OWNERS = Variable.get("dag_owners", deserialize_json=True).get(DAG_NAME, ["Unknown"])

README_PATH = os.path.join(repo_path, 'collisions/Readme.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

default_args = {
    "owner": ",".join(DAG_OWNERS),
    "depends_on_past": False,
    "start_date": pendulum.datetime(2024, 4, 12, tz="America/Toronto"),
    "email_on_failure": False,
    "retries": 0,
    "on_failure_callback": task_fail_slack_alert,
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    #loosely coupled with the two replicator DAGs which are externally triggered at 430am
    schedule='0 4 * * *',
    doc_md=DOC_MD,
    tags=["replicator", "data_checks"]
)
def replicator_DAG():

    @task()
    def tables_to_copy():
        """This task finds all the replicators from `replicators` airflow variable,
        and then finds all the tables listed for replication by looking at the Airflow variable
        listed in the `tables` key item in the replicator dictionaries."""

        #a list of the replicators
        replicators = Variable.get('replicators', deserialize_json=True)
    
        #extract source tables from airflow variables
        tables_to_copy = []
        for _, dag_items in replicators.items():
            tables = Variable.get(dag_items['tables'], deserialize_json=True)
            src_tables = [tbl[0] for tbl in tables]
            tables_to_copy = tables_to_copy + src_tables

        #get only source table names
        return tables_to_copy

    @task()
    def updated_tables(ds):
        """This task finds tables in `move_staging` with comments
        indicating they are up to date ("last updated on {ds}")."""

        updated_tables_sql = """
        SELECT pgn.nspname::text || '.' || pgc.relname::text, pgd.description
        FROM pg_description AS pgd
        JOIN pg_class AS pgc ON pgd.objoid = pgc.oid
        JOIN pg_namespace pgn ON pgc.relnamespace = pgn.oid
        WHERE
            pgn.nspname = 'move_staging'
            AND pgc.relkind = 'r'
            AND pgd.description ILIKE %s"""

        con = PostgresHook("collisions_bot").get_conn()
        with con.cursor() as cur:
            cur.execute(updated_tables_sql, (f'%Last updated on {ds}%',))
            updated_tables = [tbl[0] for tbl in cur.fetchall()]

        return updated_tables

    @task()
    def not_copied(updated_tables: list, tables_to_copy: list, **context):
        """This task finds tables which are up to date according to their comments in `move_staging` schema
        but are not being copied by the replicator due to not being included in the Airflow variables."""

        failures = [value for value in updated_tables if value not in tables_to_copy]
        if failures != []:
            #send message with details of failure using task_fail_slack_alert
            failure_extra_msg = [
                "The following tables are up to date in `move_staging` but not being copied by bigdata replicators:",
                sorted(failures)
            ]
            context.get("task_instance").xcom_push(key="extra_msg", value=failure_extra_msg)
            raise AirflowFailException('There were up to date tables in `move_staging` which were not copied by bigdata replicators.')

    @task()
    def not_up_to_date(updated_tables: list, tables_to_copy: list, **context):
        """This task finds tables which are being copied by the bigdata replicator via Airflow variables
        which are not up to date according to their comments in `move_staging` schema."""

        failures = [value for value in tables_to_copy if value not in updated_tables]
        if failures != []:
            #send message with details of failure using task_fail_slack_alert
            failure_extra_msg = [
                "The following tables are being copied by bigdata replicators, but are not up to date in `move_staging`:" ,
                sorted(failures)
            ]
            context.get("task_instance").xcom_push(key="extra_msg", value=failure_extra_msg)
            raise AirflowFailException('There were tables copied from `move_staging` by bigdata replicators which were not up to date.')

    @task()
    def outdated_remove(ds, **context):
        """This task finds outdated tables in `move_staging` based on
         comments not matching "last updated on {ds}"."""

        outdated_tables_sql = """
        SELECT pgn.nspname::text || '.' || pgc.relname::text, pgd.description
        FROM pg_namespace AS pgn
        JOIN pg_class AS pgc ON pgc.relnamespace = pgn.oid
        LEFT JOIN pg_description AS pgd ON pgd.objoid = pgc.oid
        WHERE
            pgn.nspname = 'move_staging'
            AND (
                pgd.description NOT ILIKE %s
                OR pgd.description IS NULL
            )
            AND pgc.relkind = 'r';"""

        con = PostgresHook("collisions_bot").get_conn()
        with con.cursor() as cur:
            cur.execute(outdated_tables_sql, (f'%Last updated on {ds}%',))
            failures = [tbl[0] for tbl in cur.fetchall()]

        if failures != []:
            #send message with details of failure using task_fail_slack_alert
            failure_extra_msg = [
                "The following tables in `move_staging` are outdated and should be purged:" ,
                sorted(failures)
            ]
            context.get("task_instance").xcom_push(key="extra_msg", value=failure_extra_msg)
            raise AirflowFailException('There were outdated tables in bigdata `move_staging` schema.')

    updated_tables, tables_to_copy = updated_tables(), tables_to_copy()
    wait_for_external_trigger() >> (
        not_copied(updated_tables, tables_to_copy),
        not_up_to_date(updated_tables, tables_to_copy),
        outdated_remove()
    )
    
replicator_DAG()