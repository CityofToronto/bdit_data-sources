r"""### Daily Ecocounter Data Pull DAG
Pipeline to pull ecocounter daily data and insert them into Postgres tables using Python Operator.
Also runs SQL data-checks on the pulled data, checking row count and distinct flow_ids compared to a lookback period.
"""

#add logging, unknown ids formatting
#start date and end date?

import sys
import os
import pendulum
from datetime import timedelta
import dateutil.parser
import logging
from configparser import ConfigParser

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.macros import ds_add
from airflow.exceptions import AirflowSkipException

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert, send_slack_msg
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
    from volumes.ecocounter.pull_data_from_api import (
        getToken, getSites, getChannelData, siteIsKnownToUs, insertSite,
        insertFlow, flowIsKnownToUs, truncateFlowSince, insertFlowCounts,
        getKnownSites, getKnownChannels
    )
except:
    raise ImportError("Cannot import DAG helper functions.")

DAG_NAME = 'ecocounter_pull'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

CONFIG_PATH = '/data/airflow/data_scripts/volumes/ecocounter/.api-credentials.config'

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
#LOGGER.setLevel('DEBUG')

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 3, 20, tz="America/Toronto"),
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
    template_searchpath=os.path.join(repo_path,'dags/sql'),
    catchup=False,
    tags=["ecocounter", "data_pull", "data_checks"],
    doc_md=__doc__
)
def pull_ecocounter_dag():

    @task()
    def update_sites_and_flows(**context):
        config = ConfigParser()
        config.read(CONFIG_PATH)
        token = getToken(CONFIG_PATH)
        eco_postgres = PostgresHook("ecocounter_bot")

        new_sites, new_flows = [], []
        with eco_postgres.get_conn() as conn:
            for site in getSites(token):
                site_id, site_name = site['id'], site['name']
                if not siteIsKnownToUs(site_id, conn):
                    insertSite(conn, site_id, site_name, site['longitude'], site['latitude'])
                    new_sites.append({
                        'site_id': site_id,
                        'site_name': site_name
                    })
                for channel in site['channels']:
                    channel_id, channel_name = channel['id'], channel['name']
                    if not flowIsKnownToUs(channel['id'], conn):
                        insertFlow(conn, channel_id, site_id, channel_name, channel['interval'])
                        new_flows.append({
                            'site_id': site_id,
                            'channel_id': channel_id,
                            'flow_name': channel_name
                        })

        if new_sites != []:
            LOGGER.info('New sites added: %s', new_sites)
            extra_msg = '\n'.join([str(item) for item in new_sites])
            send_slack_msg(
                context=context,
                msg=f"There were new site_ids when pulling ecocounter data :eco-counter:",
                attachments=[{"text": extra_msg}]
            )
        if new_flows != []:
            LOGGER.info('New flows added: %s', new_flows)
            extra_msg = '\n'.join([str(item) for item in new_flows])
            send_slack_msg(
                context=context,
                msg=f"There were new channel_ids when pulling ecocounter data :eco-counter:",
                attachments=[{"text": extra_msg}]
            )
        if new_flows == [] and new_sites == []:
            #Mark skipped to visually identify which days added new sites or flows in the UI.
            raise AirflowSkipException('No new sites or channels to add. Marking task as skipped.')

    @task(trigger_rule='none_failed')
    def pull_ecocounter(ds):
        config = ConfigParser()
        config.read(CONFIG_PATH)
        token = getToken(CONFIG_PATH)
        eco_postgres = PostgresHook("ecocounter_bot")
        
        start_time = dateutil.parser.parse(str(ds))
        end_time = dateutil.parser.parse(str(ds_add(ds, 1)))
        LOGGER.info(f'Pulling data from {start_time} to {end_time}.')

        with eco_postgres.get_conn() as conn:
            for site_id in getKnownSites(conn):
                LOGGER.debug(f'Starting on site {site_id}.')
                #if site_id != 300028589:
                #    continue
                for channel_id in getKnownChannels(conn, site_id):
                    LOGGER.debug(f'Starting on flow {channel_id} for site {site_id}.')
                    # empty the count table for this flow
                    truncateFlowSince(channel_id, conn, start_time, end_time)          
                    # and fill it back up!
                    LOGGER.debug(f'Fetching data for flow {channel_id}.')
                    counts = getChannelData(token, channel_id, start_time, end_time)
                    #convert response into a tuple for inserting
                    volume=[]
                    for count in counts:
                        row=(channel_id, count['date'], count['counts'])
                        volume.append(row)
                    insertFlowCounts(conn, volume)
                    LOGGER.debug(f'Data inserted for flow {channel_id} of site {site_id}.')
                LOGGER.info(f'Data inserted for site {site_id}.')
          
    @task_group(
        tooltip="Tasks to check critical data quality measures which could warrant re-running the DAG."
    )
    def data_checks():
        data_check_params = {
            "table": "gwolofs.counts",
            "lookback": '60 days',
            "dt_col": 'datetime_bin',
            "threshold": 0.7
        }
        check_volume = SQLCheckOperatorWithReturnValue(
            task_id="check_volume",
            sql="select-row_count_lookback.sql",
            conn_id="ecocounter_bot",
            retries=0,
            params=data_check_params | {"col_to_sum": 'volume'},
        )
        check_volume.doc_md = '''
        Compare the row count today with the average row count from the lookback period.
        '''

        check_distinct_flow_ids = SQLCheckOperatorWithReturnValue(
            task_id="check_distinct_flow_ids",
            sql="select-sensor_id_count_lookback.sql",
            conn_id="ecocounter_bot",
            retries=0,
            params=data_check_params | {
                    "id_col": "flow_id",
                    "threshold": 0.7
                },
        )
        check_distinct_flow_ids.doc_md = '''
        Compare the count of flow_ids appearing in today's pull vs the lookback period.
        '''

        check_volume
        check_distinct_flow_ids

    update_sites_and_flows() >> pull_ecocounter() >> data_checks()

pull_ecocounter_dag()