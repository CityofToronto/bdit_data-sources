r"""### Daily vds Data Check DAG
Pipeline to run additional SQL data quality checks on daily vds pull.
Put 'yellow card' checks which would not warrant the re-running of the data_pull pipeline,
ie. issues which suggest field maintenance of sensors required. 
"""
import sys
import os
import logging
import pendulum
from datetime import timedelta
from functools import partial

from airflow.sdk import dag, Variable
from airflow.sensors.external_task import ExternalTaskSensor

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
<<<<<<< HEAD
    from airflow3_bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, slack_alert_data_quality, get_readme_docmd
    from airflow3_bdit_dag_utils.utils.custom_operators import SQLCheckOperatorWithReturnValue
=======
    from dags.dag_owners import owners
    from bdit_dag_utils.utils.dag_functions import (
        task_fail_slack_alert, slack_alert_data_quality, get_readme_docmd
    )
    from bdit_dag_utils.utils.custom_operators import SQLCheckOperatorWithReturnValue
>>>>>>> 2a00a38e (move dag_owners to local file #1211)
except:
    raise ImportError("Cannot import DAG helper functions.")

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DAG_NAME = 'vds_check'
DAG_OWNERS = owners.get(DAG_NAME, ["Unknown"])

README_PATH = os.path.join(repo_path, 'volumes/vds/readme.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past':False,
    'start_date': pendulum.datetime(2024, 5, 10, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': partial(task_fail_slack_alert, use_proxy = True)
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule='0 5 * * FRI', # Run at 5 AM on Friday
    catchup=False,
    template_searchpath=os.path.join(repo_path,'volumes/vds/sql/select'),
    tags=["bdit_data-sources", "vds", "data_checks", "weekly"],
    doc_md=DOC_MD
)
def vds_check_dag():

    t_upstream_done = ExternalTaskSensor(
        task_id="starting_point",
        external_dag_id="vds_pull_vdsdata",
        external_task_id="done",
        poke_interval=3600, #retry hourly
        mode="reschedule",
        timeout=86400, #one day
        execution_delta=timedelta(days=-6, hours=1) #pull_vds scheduled at '0 4 * * *'
    )

    check_missing_centreline_id = SQLCheckOperatorWithReturnValue(
        on_failure_callback=partial(slack_alert_data_quality, use_proxy=True),
        task_id="check_missing_centreline_id",
        sql="select-missing_centreline.sql",
        conn_id="vds_bot"
    )
    check_missing_centreline_id.doc_md = '''
    Identify intersections which appeared within the lookback period that did not appear today.
    '''

    check_missing_expected_bins = SQLCheckOperatorWithReturnValue(
        on_failure_callback=partial(slack_alert_data_quality, use_proxy=True),
        task_id="check_missing_expected_bins",
        sql="select-missing_expected_bins.sql",
        conn_id="vds_bot"
    )
    check_missing_expected_bins.doc_md = '''
    Identify intersections which appeared within the lookback period that did not appear today.
    '''

    t_upstream_done >> [
        check_missing_centreline_id,
        check_missing_expected_bins
    ]

vds_check_dag()