import os
import sys
import json
import logging
import requests
import pendulum
import pandas as pd
from datetime import datetime, timedelta
import dateutil.parser

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.exceptions import AirflowFailException

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DAG_NAME = 'open_data_checks'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ['Unknown'])

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_functions import task_fail_slack_alert, get_readme_docmd, send_slack_msg

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

BASE_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca"

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    max_active_runs=1,
    #doc_md=DOC_MD,
    tags=['bdit_data-sources', 'open_data', 'data_check'],
    schedule='0 0 5 * *', #fifth day of every month
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
        p = {"id": od_id}
        package_show_url = BASE_URL + "/api/3/action/package_show"
        package = requests.get(package_show_url, params = p).json()
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
            days_old=(datetime.now() - last_refreshed).days
            md_url=f"<https://open.toronto.ca/dataset/{od_id}/|`{od_id}`>"
            failure_msg = f"{md_url} is out of date. Last refreshed: `{last_refreshed}` (days old: {days_old})."
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
    
    @task()
    def usage_stats(ids, **context):
        # file clicks are also available under a different non-datastore resource
        resource_id = "eb98a22e-b9d2-4b30-ab1c-fdc9c3fcf8d3" #'Page Views and Time Based Metrics.csv'
        datastore_search_url = BASE_URL + "/api/3/action/datastore_search"
        last_month = pendulum.now().subtract(months=1)
        page_urls = [f"open.toronto.ca/dataset/{id}/" for id in ids]
        p = {
            "resource_id": resource_id,
            "filters": json.dumps({
                "Link Source -Page URL": page_urls,
                "Month": last_month.format('YMM')
            })
        }
        resource_search_data = requests.get(datastore_search_url, params = p).json()["result"]
        # Convert to DataFrame and clean
        df = pd.DataFrame(resource_search_data['records'])
        df["Id"] = df["Link Source -Page URL"].str.extract(r'open.toronto.ca/dataset/([^/]+)')
        
        #filter columns and sort
        df_sorted = df[["Id", "Sessions", "Users", "Views", "Avg Session Duration (Sec)"]]
        df_sorted = df_sorted.sort_values(by="Views", ascending=False)
        dt_md = df_sorted.to_markdown(index = False, tablefmt="slack")
        
        send_slack_msg(
            context=context,
            msg=f":open_data_to: page view analytics from `{last_month.format('MMMM Y')}`:```{dt_md}```"
        )

    ids=fetch_datasets()
    check_freshness.expand(od_id = ids) >> freshness_message(ids)
    usage_stats(ids)

od_check_dag()