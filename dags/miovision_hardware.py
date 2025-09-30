import sys
import os
import pendulum
from datetime import timedelta

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_owners import owners
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, get_readme_docmd
    from volumes.miovision.api.configuration_info import (
        get_cameras, get_configuration_dates
    )
except:
    raise ImportError("Cannot import DAG helper functions.")

DAG_NAME = 'miovision_hardware'
DAG_OWNERS = owners.get(DAG_NAME, ["Unknown"])

README_PATH = os.path.join(repo_path, 'volumes/miovision/api/readme.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 12, 5, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule='0 2 * * *',
    catchup=False,
    tags=["miovision", "data_pull"],
    doc_md=DOC_MD
)
def pull_miovision_dag():

    @task(retries = 1)
    def pull_config_dates():
        mio_postgres = PostgresHook("miovision_api_bot")
        with mio_postgres.get_conn() as conn:
            get_configuration_dates(conn)

    @task(retries = 1)
    def pull_camera_details():
        mio_postgres = PostgresHook("miovision_api_bot")
        with mio_postgres.get_conn() as conn:
            get_cameras(conn)

    pull_config_dates()
    pull_camera_details()

pull_miovision_dag()