r"""### Daily Ecocounter Data Check DAG
Pipeline to run additional SQL data quality checks on daily ecocounter pull.
Put 'yellow card' checks which would not warrant the re-running of the data_pull pipeline,
ie. issues which suggest field maintenance of sensors required. 
"""
import sys
import os
import logging
import pendulum
from datetime import timedelta

from airflow.sdk import dag
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, slack_alert_data_quality, get_readme_docmd
    from bdit_dag_utils.utils.custom_operators import SQLCheckOperatorWithReturnValue
except:
    raise ImportError("Cannot import DAG helper functions.")

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DAG_NAME = 'ecocounter_check'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

README_PATH = os.path.join(repo_path, 'volumes/ecocounter/readme.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 4, 14, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule='5 10 * * MON', # Run at 10:05 AM on Monday
    catchup=True,
    template_searchpath=os.path.join(repo_path, 'volumes/ecocounter/data_checks'),
    tags=["ecocounter", "data_checks", "weekly"],
    doc_md=DOC_MD
)
def ecocounter_check_dag():

    t_upstream_done = ExternalTaskSensor(
        task_id="starting_point",
        external_dag_id="ecocounter_pull",
        external_task_id="done",
        poke_interval=3600, #retry hourly
        mode="reschedule",
        timeout=86400, #one day
        #when this DAG runs on Monday (day 7 - at the end of it's week long schedule interval),
        #it should check for the Sunday (day 6) _pull DAG, which gets executed on the Monday.
        execution_delta=timedelta(days=-6, minutes=5) #pull_ecocounter scheduled at '0 10 * * *'
    )

    check_site_outages = SQLCheckOperatorWithReturnValue(
        on_failure_callback=slack_alert_data_quality,
        task_id="check_site_outages",
        sql="select-ongoing_outages.sql",
        conn_id="ecocounter_bot",
        params={
            "lookback": '100 days',
            "min_duration": '1 days'
        }
    )
    check_site_outages.doc_md = '''
    Identify sites & flows which appeared within the lookback period that did not appear today.
    '''
    
    check_unvalidated_sites = SQLCheckOperatorWithReturnValue(
        on_failure_callback=slack_alert_data_quality,
        task_id="check_unvalidated_sites",
        sql="select-unvalidated_sites.sql",
        conn_id="ecocounter_bot",
        params={
            "lookback": '7 days'
        }
    )
    check_unvalidated_sites.doc_md = '''
    Identify new sites that have not been validated by a human with counts in the last 7 days.
    '''
    t_upstream_done >> [
        check_site_outages,
        check_unvalidated_sites
    ]

ecocounter_check_dag()