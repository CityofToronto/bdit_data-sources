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
from airflow.sdk import dag, task, Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.models.taskinstance import TaskInstance

# import custom operators and helper functions
repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_owners import owners
# pylint: disable=wrong-import-position
from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, get_readme_docmd
# pylint: enable=import-error

DAG_NAME = 'replicator_table_check'
DAG_OWNERS = owners.get(DAG_NAME, ["Unknown"])

README_PATH = os.path.join(repo_path, 'collisions/readme.md')
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
    schedule='0 7 * * *',
    doc_md=DOC_MD,
    tags=["replicator", "data_checks"]
)
def replicator_DAG():

    #backfill is meaningless since comparing to current table comments.
    no_backfill = LatestOnlyOperator(task_id = 'no_backfill')

    @task()
    def tables_to_copy(ti: TaskInstance | None = None):
        """This task finds all the replicators from `replicators` airflow variable,
        and then finds all the tables listed for replication by looking at the Airflow variable
        listed in the `tables` key item in the replicator dictionaries."""

        #a list of the replicators
        replicators = Variable.get('replicators', deserialize_json=True)
    
        #extract source tables from airflow variables
        src_tables = []
        dest_tables = []
        for _, dag_items in replicators.items():
            tables = Variable.get(dag_items['tables'], deserialize_json=True)
            temp_src = [tbl[0] for tbl in tables]
            src_tables = src_tables + temp_src
            #the last element is the destination for the table comment with date updated.
            temp_dest = [tbl[-1] for tbl in tables]
            dest_tables = dest_tables + temp_dest

        ti.xcom_push(key="src_tables", value=src_tables)
        ti.xcom_push(key="dest_tables", value=dest_tables)
    
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

        con = PostgresHook("replicator_bot").get_conn()
        with con.cursor() as cur:
            cur.execute(updated_tables_sql, (f'%Last updated on {ds}%',))
            updated_tables = [tbl[0] for tbl in cur.fetchall()]

        return updated_tables
    
    @task
    def not_copied_dest(ds = None, ti: TaskInstance | None = None):
        dest_tables = ti.xcom_pull(task_ids="tables_to_copy", key="dest_tables")
        sql = """
        SELECT substring(pgd.description, '\d{4}-\d{2}-\d{2}') AS date_updated
        FROM pg_description AS pgd
        JOIN pg_class AS pgc ON pgd.objoid = pgc.oid
        JOIN pg_namespace pgn ON pgc.relnamespace = pgn.oid
        WHERE
            pgn.nspname = %s
            AND pgc.relname = %s
        """
        con = PostgresHook("replicator_bot").get_conn()
        not_updated = []
        for table in dest_tables:
            sch, tbl = table.split('.')
            with con.cursor() as cur:
                cur.execute(sql, (sch, tbl))
                result = cur.fetchone()[0]
                if result != ds:
                    not_updated = not_updated + [f"`{table}` last updated: {result}"]
        
        if not_updated != []:
            #send message with details of failure using task_fail_slack_alert
            failure_extra_msg = [
                "The following replication destination tables are not up to date:",
                sorted(not_updated)
            ]
            ti.xcom_push(key="extra_msg", value=failure_extra_msg)
            raise AirflowFailException('There are tables which were not replicated successfully.')

        return 0
    
    @task()
    def not_copied_src(updated_tables: list, ti: TaskInstance | None = None):
        """This task finds tables which are up to date according to their comments in `move_staging` schema
        but are not being copied by the replicator due to not being included in the Airflow variables."""
        tables_to_copy = ti.xcom_pull(task_ids="tables_to_copy", key="src_tables")
        failures = [value for value in updated_tables if value not in tables_to_copy]
        if failures != []:
            #send message with details of failure using task_fail_slack_alert
            failure_extra_msg = [
                "The following tables are up to date in `move_staging` but not being copied by bigdata replicators:",
                sorted(failures)
            ]
            ti.xcom_push(key="extra_msg", value=failure_extra_msg)
            raise AirflowFailException('There were up to date tables in `move_staging` which were not copied by bigdata replicators.')

    @task()
    def not_up_to_date(updated_tables: list, ti: TaskInstance | None = None):
        """This task finds tables which are being copied by the bigdata replicator via Airflow variables
        which are not up to date according to their comments in `move_staging` schema."""
        tables_to_copy = ti.xcom_pull(task_ids="tables_to_copy", key="src_tables")
        failures = [value for value in tables_to_copy if value not in updated_tables]
        if failures != []:
            #send message with details of failure using task_fail_slack_alert
            failure_extra_msg = [
                "The following tables are being copied by bigdata replicators, but are not up to date in `move_staging`:" ,
                sorted(failures)
            ]
            ti.xcom_push(key="extra_msg", value=failure_extra_msg)
            raise AirflowFailException('There were tables copied from `move_staging` by bigdata replicators which were not up to date.')

    @task()
    def outdated_remove(ds, ti: TaskInstance | None = None):
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

        con = PostgresHook("replicator_bot").get_conn()
        with con.cursor() as cur:
            cur.execute(outdated_tables_sql, (f'%Last updated on {ds}%',))
            failures = [tbl[0] for tbl in cur.fetchall()]

        if failures != []:
            #send message with details of failure using task_fail_slack_alert
            failure_extra_msg = [
                "The following tables in `move_staging` are outdated and should be purged:" ,
                sorted(failures)
            ]
            ti.xcom_push(key="extra_msg", value=failure_extra_msg)
            raise AirflowFailException('There were outdated tables in bigdata `move_staging` schema.')
    
    updated_tables = updated_tables()
    no_backfill >> tables_to_copy() >> updated_tables >> (
        not_copied_dest(),
        not_copied_src(updated_tables),
        not_up_to_date(updated_tables),
        outdated_remove()
    )
    
replicator_DAG()