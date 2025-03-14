import os
import sys
import logging
import requests
from datetime import datetime, timedelta
import dateutil.parser

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.models.taskinstance import TaskInstance
from airflow.exceptions import AirflowFailException

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DAG_NAME = 'open_data_checks'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ['Unknown'])

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_functions import task_fail_slack_alert, get_readme_docmd

#README_PATH = os.path.join(repo_path, 'events/road_permits/readme.md')
#DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True, #Allow for progressive longer waits between retries
    'on_failure_callback': task_fail_slack_alert,
}

BASE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show"

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    max_active_runs=1,
    #doc_md=DOC_MD,
    tags=['bdit_data-sources', 'open_data', 'data_check'],
    schedule='@monthly', #monthly
    catchup=False,
)

def od_check_dag():
    @task
    def fetch_datasets():
        return Variable.get('open_data_ids', deserialize_json=True)
        
    @task(map_index_template="{{ od_id }}")
    def check_freshness(od_id = None, **context):
        #name mapped task
        context["od_id"] = od_id
        
        #get open data metadata
        params = {"id": od_id}
        package = requests.get(BASE_URL, params = params).json()
        try:
            result = package.get('result')
            refresh_rate = result.get('refresh_rate')
            last_refreshed = dateutil.parser.parse(result.get('last_refreshed'))
        except KeyError as e:
            LOGGER.error("Problem retrieving Open Data portal info.")
            raise AirflowFailException(e)
        LOGGER.info("`%s` last_refreshed: %s", od_id, last_refreshed)
        
        if refresh_rate == 'Daily':
            max_offset = timedelta(days=3)
        elif refresh_rate == 'Monthly':
            max_offset = timedelta(days=60)
        else:
            max_offset = timedelta(days=60)
        
        if last_refreshed < datetime.now() - max_offset:
            failure_msg = f"<https://open.toronto.ca/dataset/{od_id}/|`{od_id}`> is out of date. Last refreshed: `{last_refreshed}` (days old: {(datetime.now() - last_refreshed).days})."
            context["task_instance"].xcom_push(key="failure_msg", value=failure_msg)
    
    @task(
        retries=0,
        trigger_rule='all_done',
        doc_md="""A status message to report any failures from the `check_freshness` task."""
    )
    def freshness_message(ids, ti: TaskInstance | None = None):
        failures = []
        #iterate through mapped tasks to find any failure messages
        for m_i in range(0, len(ids)):
            failure_msg = ti.xcom_pull(task_ids="check_freshness", key="failure_msg", map_indexes=m_i)
            if failure_msg is not None:
                failures.append(failure_msg)
        if failures != []:
            failure_extra_msg = ['One or more Open Data pages is outdated:', failures]
            ti.xcom_push(key="extra_msg", value=failure_extra_msg)
            raise AirflowFailException('One or more Open Data pages is outdated.')
    
    ids=fetch_datasets()
    check_freshness.expand(od_id = ids) >> freshness_message(ids)

od_check_dag()