"""### open_data_checks DAG

- open data uids are stored in Airflow variable: `open_data_ids`
- `check_freshness` checks if datasets are outdated versus their stated refresh times
- `usage_stats` reports usage stats from the previous month based on stats released to Open Data 
"""
import os
import io
import sys
import json
import zipfile
import logging
import requests
import pendulum
import pandas as pd
from datetime import datetime, timedelta
import dateutil.parser

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowFailException

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DAG_NAME = 'open_data_checks'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ['Unknown'])

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, send_slack_msg

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
    template_searchpath=os.path.join(repo_path,'ttc/gtfs'),
    doc_md=__doc__,
    tags=['bdit_data-sources', 'open_data', 'data_check'],
    schedule='0 0 5 * *', #fifth day of every month
    catchup=False,
)

def gtfs_pull():
    @task()
    def download_gtfs(**context):
        
        #get open data metadata
        od_id = 'ttc-routes-and-schedules'
        params = { "id": od_id}
        package = requests.get(BASE_URL, params = params).json()
        
        try:
            result = package.get('result')
            download_url = result['resources'][0].get('url')
            last_refreshed = result.get('last_refreshed')
        except KeyError as e:
            LOGGER.error("Problem retrieving Open Data portal info.")
            raise AirflowFailException(e)
        LOGGER.info("`%s` last_refreshed: %s", od_id, last_refreshed)
        
        gtfs_download=requests.get(download_url)
       
        if gtfs_download.status_code != 200:
            raise Exception('Error' + str(gtfs_download.status_code))
    
        last_refreshed = last_refreshed.translate(str.maketrans('','','-:'))
        last_refreshed = last_refreshed.replace(' ', '_')
        dir = "/data/airflow/open_data/gtfs/" + last_refreshed
        os.makedirs(dir)
        
        z = zipfile.ZipFile(io.BytesIO(gtfs_download.content))
        z.extractall(dir)
        
        return dir
    
    @task.bash(
        env = {
            'HOST': '{{ conn.gtfs_bot.host }}',
            'LOGIN': '{{ conn.gtfs_bot.login }}',
            'PGPASSWORD': '{{ conn.gtfs_bot.password }}',
            'EXPORT_PATH': {{ti.xcom_pull(key='return_value', task_ids='download_gtfs')[1]}}
        }
    )
    def upload_feed():
        return "upload_feed.sh"

    download_gtfs() >> upload_feed()
    
gtfs_pull()