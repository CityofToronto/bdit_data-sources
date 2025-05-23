r"""### Daily Miovision Data Check DAG
Pipeline to run additional SQL data quality checks on daily miovision pull.
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

DAG_NAME = 'miovision_check'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

README_PATH = os.path.join(repo_path, 'volumes/miovision/api/readme.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past':False,
    'start_date': pendulum.datetime(2023, 12, 18, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule='0 4 * * MON', # Run at 4 AM on Monday
    catchup=False,
    template_searchpath=os.path.join(repo_path,'volumes/miovision/sql/data_checks'),
    tags=["miovision", "data_checks", "weekly"],
    doc_md=DOC_MD
)
def miovision_check_dag():

    t_upstream_done = ExternalTaskSensor(
        task_id="starting_point",
        external_dag_id="miovision_pull",
        external_task_id="done",
        poke_interval=3600, #retry hourly
        mode="reschedule",
        timeout=86400, #one day
        #when this DAG runs on Monday (day 7 - at the end of it's week long schedule interval),
        #it should check for the Sunday (day 6) _pull DAG, which gets executed on the Monday.
        execution_delta=timedelta(days=-6, hours=1) #pull_miovision scheduled at '0 3 * * *'
    )

    check_distinct_intersection_uid = SQLCheckOperatorWithReturnValue(
        on_failure_callback=slack_alert_data_quality,
        task_id="check_intersection_outages",
        sql="select-ongoing_intersection_outages.sql",
        conn_id="miovision_api_bot",
        params={
            "lookback": '60 days',
            "min_duration": '0 days'
        }
    )
    check_distinct_intersection_uid.doc_md = '''
    Identify intersections which appeared within the lookback period that did not appear today.
    '''

    check_open_anomalous_ranges = SQLCheckOperatorWithReturnValue(
        on_failure_callback=slack_alert_data_quality,
        task_id="check_open_anomalous_ranges",
        sql="select-open_issues.sql",
        conn_id="miovision_api_bot"
    )
    check_open_anomalous_ranges.doc_md = '''
    Identify open ended gaps that have non-zero volumes in the last week and notify DAG owners so ranges don't get stale.
    '''

    check_monitor_intersection_movements = SQLCheckOperatorWithReturnValue(
        on_failure_callback=slack_alert_data_quality,
        task_id="check_monitor_intersection_movements",
        sql="select-monitor_intersection_movements.sql",
        conn_id="miovision_api_bot"
    )
    check_monitor_intersection_movements.doc_md = '''
    Identify high volume movements missing from intersection_movements table and notify.
    '''

    t_upstream_done >> [
        check_distinct_intersection_uid,
        check_open_anomalous_ranges,
        check_monitor_intersection_movements
    ]

miovision_check_dag()