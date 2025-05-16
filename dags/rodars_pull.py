import os
import sys
import pendulum
from functools import partial

from airflow.decorators import dag, task, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

DAG_NAME = 'rodars_pull'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ['Unknown'])

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)

from events.road_permits.rodars_functions import (
    fetch_and_insert_issue_data, fetch_and_insert_location_data
)
from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, slack_alert_data_quality, get_readme_docmd
from bdit_dag_utils.utils.custom_operators import SQLCheckOperatorWithReturnValue

README_PATH = os.path.join(repo_path, 'events/road_permits/readme.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 11, 27, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
    'retry_exponential_backoff': True, #Allow for progressive longer waits between retries
    'on_failure_callback': partial(task_fail_slack_alert, use_proxy = True),
    'catchup': True,
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath=[
        os.path.join(repo_path,'events/road_permits/sql')
    ],
    doc_md=DOC_MD,
    tags=['bdit_data-sources', 'rodars', 'pull', 'itsc_central'],
    schedule='0 4 * * *' #daily at 4am
)

def rodars_dag():
    @task(retries = 2, retry_delay = pendulum.duration(hours=1))
    def pull_rodars_issues(ds = None):
        "Get RODARS data from ITSC and insert into bigdata `congestion_events.itsc_issues`"
        itsc_bot = PostgresHook('itsc_postgres')
        events_bot = PostgresHook('events_bot')
        fetch_and_insert_issue_data(select_conn=itsc_bot, insert_conn=events_bot, start_date=ds)
    
    @task(retries = 2, retry_delay = pendulum.duration(hours=1))
    def pull_rodar_locations(ds = None):
        "Get RODARS data from ITSC and insert into bigdata `congestion_events.itsc_issue_locations`"
        itsc_bot = PostgresHook('itsc_postgres')
        events_bot = PostgresHook('events_bot')
        fetch_and_insert_location_data(select_conn=itsc_bot, insert_conn=events_bot, start_date=ds)

    @task_group
    def data_checks():
        check_src_issue_count = SQLCheckOperatorWithReturnValue(
            task_id="check_src_issue_count",
            sql='''
                SELECT
                    True AS "_check",
                    COUNT(DISTINCT divisionid::text || issueid::text) AS issue_count
                FROM public.issuedata
                WHERE
                    --there could be issues created right after we pulled them
                    timestamputc < '{{ ds }}'::date + interval '1 day'
                    AND divisionid IN (
                        8048, --rodars new
                        8014 --rodars (old)
                    )''',
            conn_id="itsc_postgres"
        )
        check_src_issue_count.doc_md = "Check the source issue count."
        
        check_dest_issue_count = SQLCheckOperatorWithReturnValue(
            task_id="check_dest_issue_count",
            sql='''
                SELECT
                    COUNT(DISTINCT divisionid::text || issueid::text)
                    = {{ti.xcom_pull(key='return_value', task_ids='data_checks.check_src_issue_count')[1]}} AS "_check",
                    'Bigdata count: ' || TO_CHAR(COUNT(DISTINCT divisionid::text || issueid::text), '999,999,999,999,999')
                    || ', ITSC count: '
                    || TO_CHAR({{ti.xcom_pull(key='return_value', task_ids='data_checks.check_src_issue_count')[1]}}, '999,999,999,999,999')
                    AS description
                FROM congestion_events.rodars_issues
                ''',
            conn_id="events_bot"
        )
        check_src_issue_count.doc_md = "Check the dest issue count vs the source issue count."

        check_src_issue_count >> check_dest_issue_count
        
    #these tasks are not dependent, but this helps so only one fails at a time
    pull_rodars_issues() >> pull_rodar_locations() >> data_checks()

rodars_dag()
