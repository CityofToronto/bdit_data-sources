import sys
import os
import pendulum
from datetime import timedelta

from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, get_readme_docmd
    #from volumes.miovision.api.configuration_info import 
except:
    raise ImportError("Cannot import DAG helper functions.")

DAG_NAME = 'miovision_validation'
DAG_OWNERS = ["gabe"]

README_PATH = os.path.join(repo_path, 'volumes/miovision/api/readme.md')
#DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)
DOC_MD = ""

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 11, 10, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule='0 7 * * *',
    catchup=False,
    tags=["miovision", "data_pull"],
    template_searchpath=os.path.join(repo_path,'volumes/miovision/sql/validation'),
    doc_md=DOC_MD
)
def miovision_validation_dag():
    spectrum_studies = SQLExecuteQueryOperator(
        task_id="insert_spectrum_studies",
        sql="SELECT miovision_validation.insert_spectrum_studies();",
        conn_id="miovision_validation_bot"
    )
    
    api_counts = SQLExecuteQueryOperator(
        task_id="insert_api_counts",
        sql="SELECT miovision_validation.insert_processed_counts();",
        conn_id="miovision_validation_bot"
    )

    insert_valid_legs = SQLExecuteQueryOperator(
        task_id="insert_valid_legs",
        sql="insert-valid_legs.sql",
        conn_id="miovision_validation_bot"
    )
    
    insert_valid_intersections = SQLExecuteQueryOperator(
        task_id="insert_valid_intersections",
        sql="insert-valid_intersections.sql",
        conn_id="miovision_validation_bot"
    )
    
    spectrum_studies >> api_counts >> insert_valid_legs >> insert_valid_intersections

miovision_validation_dag()