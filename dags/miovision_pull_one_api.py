r"""### Daily Miovision Data Pull DAG
Pipeline to pull miovision daily data and insert them into Postgres tables using Python Operator.
Inserted data is also aggregated into 15 minute and daily summaries, and unacceptable_gaps.
Also creates new yearly/monthly partition tables if necessary and runs SQL data-checks on the
pulled data, checking row count and distinct classification_uids compared to a lookback period.
"""
import sys
import os
import pendulum
from datetime import timedelta

from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.macros import ds_add

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert, get_readme_docmd
    from volumes.miovision.api.intersection_tmc_one import run_api
    from volumes.miovision.api.pull_alert_miovision_one import run_alerts_api
except:
    raise ImportError("Cannot import DAG helper functions.")

DAG_NAME = 'miovision_pull_one_api'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get('miovision_pull', ["Unknown"])

README_PATH = os.path.join(repo_path, 'volumes/miovision/api/readme.md')
DOC_MD = get_readme_docmd(README_PATH, 'miovision_pull')

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 9, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule='0 3 * * *',
    template_searchpath=os.path.join(repo_path,'dags/sql'),
    catchup=False,
    params={
        "intersection": Param(
            default=[0],
            type="array",
            title="An array of intersection_uid (integers).",
            description="A list of intersection_uid to pull/aggregate for a single date. Default [0] will pull all intersections.",
            items={"type": "number"},
        )
    },
    tags=["miovision", "data_pull", "partition_create", "data_checks"],
    doc_md=DOC_MD
)
def pull_miovision_dag():

    @task(trigger_rule='none_failed', retries = 1)
    def pull_miovision(ds = None, **context):

        if context["params"]["intersection"] == [0]:
            INTERSECTION = ()
        else:
            INTERSECTION = tuple(context["params"]["intersection"])

        run_api(
            start_date=ds,
            end_date=ds_add(ds, 1),
            intersection=INTERSECTION,
            pull=True,
            agg=False
        )

    @task(trigger_rule='none_failed', retries = 1)
    def pull_alerts(ds):
        run_alerts_api(
            start_date=ds,
            end_date=ds_add(ds, 1)
        )
    
    pull_miovision()
    pull_alerts()

pull_miovision_dag()