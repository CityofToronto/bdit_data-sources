r"""### Daily Miovision Alert Pull DAG
Pipeline to pull miovision alerts daily and insert them into Postgres tables using Python Operator.
Inserted rows are used to extend the end time of existing rows.
"""
import sys
import os
import pendulum
from datetime import timedelta
import configparser
import dateutil.parser

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.macros import ds_add

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert, get_readme_docmd
    from volumes.miovision.api.pull_alert import pull_alerts
except:
    raise ImportError("Cannot import DAG helper functions.")

DAG_NAME = 'miovision_alerts'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

README_PATH = os.path.join(repo_path, 'volumes/miovision/api/readme.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

API_CONFIG_PATH = '/data/airflow/data_scripts/volumes/miovision/api/config.cfg'

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': True, #records combine in one direction only
    'start_date': pendulum.datetime(2024, 1, 1, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule='0 3 * * *',
    catchup=True,
    tags=["miovision", "data_pull", "alerts"],
    doc_md=DOC_MD
)
def pull_miovision_dag():

    @task()
    def pull_miovision(ds = None):       
        CONFIG = configparser.ConfigParser()
        CONFIG.read(API_CONFIG_PATH)
        api_key=CONFIG['API']
        key=api_key['key']
        start_date = dateutil.parser.parse(str(ds))
        end_date = dateutil.parser.parse(str(ds_add(ds, 1)))
        mio_postgres = PostgresHook("miovision_api_bot")

        with mio_postgres.get_conn() as conn:
            pull_alerts(conn, start_date, end_date, key)

    pull_miovision()

pull_miovision_dag()