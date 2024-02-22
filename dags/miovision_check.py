r"""### Daily Miovision Data Check DAG
Pipeline to run additional SQL data quality checks on daily miovision pull.
Put 'yellow card' checks which would not warrant the re-running of the data_pull pipeline,
ie. issues which suggest field maintenance of sensors required. 
"""
import sys
import os

from airflow.decorators import dag, task
from datetime import timedelta, datetime
from airflow.models import Variable 
from airflow.sensors.external_task import ExternalTaskSensor

import logging
import pendulum

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert, get_readme_docmd
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
except:
    raise ImportError("Cannot import DAG helper functions.")

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

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
    schedule='0 4 * * *', # Run at 4 AM local time every day
    catchup=False,
    template_searchpath=[
        os.path.join(repo_path,'volumes/miovision/sql/data_checks'),
        os.path.join(repo_path,'dags/sql')
    ],
    tags=["miovision", "data_checks"],
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
        execution_delta=timedelta(hours=1) #pull_miovision scheduled at '0 3 * * *'
    )

    check_distinct_intersection_uid = SQLCheckOperatorWithReturnValue(
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

    check_gaps = SQLCheckOperatorWithReturnValue(
        task_id="check_gaps",
        sql="""SELECT _check, summ, gaps
            FROM public.summarize_gaps_data_check(
                start_date := '{{ ds }}'::date,
                end_date := '{{ ds }}'::date,
                id_col := 'intersection_uid'::text,
                dt_col := 'datetime_bin'::text,
                sch_name := 'miovision_api'::text,
                tbl_name := 'volumes'::text,
                gap_threshold := '4 hours'::interval,
                default_bin := '1 minute'::interval
            )""",
        conn_id="miovision_api_bot"
    )
    check_gaps.doc_md = '''
    Identify gaps larger than gap_threshold in intersections with values today.
    '''

    @task.short_circuit(ignore_downstream_trigger_rules=False, retries=0) #only skip immediately downstream task
    def check_if_thursday(ds=None): #check if thursday to trigger weekly check. 
        start_date = datetime.strptime(ds, '%Y-%m-%d')
        return start_date.isoweekday() == 4
    
    check_open_anomalous_ranges = SQLCheckOperatorWithReturnValue(
        task_id="check_open_anomalous_ranges",
        sql="""WITH ars AS (
            SELECT
                uid,
                intersection_name,
                classification_uid,
                notes
            FROM miovision_api.open_issues
            WHERE last_week_volume > 0
            ORDER BY uid
        )

        SELECT
            NOT(COUNT(*) > 0) AS _check,
            CASE WHEN COUNT(*) = 1 THEN 'There is ' ELSE 'There are ' END || COUNT(*) ||
                ' open ended anomalous_range(s) with non-zero volumes in the last 7 days: (uid, intersection_name, classification_uid, notes)'
            AS summ,
            array_agg(ars || chr(10)) AS gaps
        FROM ars""",
        conn_id="miovision_api_bot"
    )
    check_open_anomalous_ranges.doc_md = '''
    Identify open ended gaps that have non-zero volumes in the last week and notify DAG owners so ranges don't get stale.
    '''

    t_upstream_done >> [
        check_distinct_intersection_uid, check_gaps,
        [check_if_thursday() >> check_open_anomalous_ranges]
    ]

miovision_check_dag()