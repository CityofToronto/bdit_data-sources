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
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, send_slack_msg, get_readme_docmd
    from bdit_dag_utils.utils.common_tasks import check_1st_of_month
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
            failure_responses = get_configuration_dates(conn)

        return failure_responses

    @task(retries = 1)
    def pull_camera_details():
        mio_postgres = PostgresHook("miovision_api_bot")
        with mio_postgres.get_conn() as conn:
            get_cameras(conn)

    @task(pre_execute=check_1st_of_month)
    def create_slack_message(response, **context):
        failure_count = len(response)
        msg_str = f"Failure to pull_config_dates on {failure_count} intersections"

        extra_msg = ""

        if failure_count > 0:

            for l in response:
                intersection_id = l['intersection_id']
                status_code = l['status_code']
                response =  l['reason']
                extra_msg += f"Intersection {intersection_id} recieved {status_code} error: {response} \n"

        send_slack_msg(context=context,
                       msg=msg_str,
                       attachments=[{"text": extra_msg}],
                       channel='slack_data_pipeline_data_quality')

    pull_camera_details()
    config_failure_responses = pull_config_dates()
    create_slack_message(response=config_failure_responses)

pull_miovision_dag()