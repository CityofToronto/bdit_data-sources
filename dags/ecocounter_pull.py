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

from airflow.decorators import dag, task, task_group
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.macros import ds_add
from airflow.exceptions import AirflowSkipException

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import (
        task_fail_slack_alert, send_slack_msg, get_readme_docmd
    )
    from dags.common_tasks import check_jan_1st
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
    from volumes.ecocounter.pull_data_from_api import (
        getToken, getSites, getFlowData, siteIsKnownToUs, insertSite,
        insertFlow, flowIsKnownToUs, truncateFlowSince, insertFlowCounts,
        getKnownSites, getKnownFlows
    )
except:
    raise ImportError("Cannot import DAG helper functions.")

DAG_NAME = 'ecocounter_pull'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

README_PATH = os.path.join(repo_path, 'volumes/ecocounter/readme.md')
DOC_MD = get_readme_docmd(README_PATH, DAG_NAME)

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
#LOGGER.setLevel('DEBUG')

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 4, 3, tz="America/Toronto"),
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
    catchup=True,
    max_active_runs=1,
    tags=["ecocounter", "data_pull", "data_checks", "partition_create"],
    doc_md=DOC_MD
)
def pull_ecocounter_dag():

    @task_group(tooltip="Tasks to check if necessary to create new partitions and if so, exexcute.")
    def check_partitions():

        create_annual_partition = PostgresOperator(
            task_id='create_annual_partitions',
            sql="""SELECT ecocounter.create_yyyy_counts_unfiltered_partition(
                    base_table := 'counts_unfiltered',
                    year_ := '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int
                )""",
            postgres_conn_id='ecocounter_bot',
            autocommit=True
        )
      
        check_jan_1st.override(task_id="check_annual_partition")() >> create_annual_partition

    @task()
    def update_sites_and_flows(**context):
        api_conn = BaseHook.get_connection('ecocounter_api_key')
        token = getToken(
            api_conn.host,
            api_conn.login,
            api_conn.password,
            api_conn.extra_dejson['secret_api_hash']
        )
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
                # what we refer to as a flow is known as a "channel" in ecocounter API.
                for flow in site['channels']:
                    flow_id, flow_name = flow['id'], flow['name']
                    if not flowIsKnownToUs(flow_id, conn):
                        insertFlow(conn, flow_id, site_id, flow_name, flow['interval'])
                        new_flows.append({
                            'site_id': site_id,
                            'flow_id': flow_id,
                            'flow_name': flow_name
                        })

        if len(new_sites) > 0:
            LOGGER.info('New sites added: %s', new_sites)
            extra_msg = '\n'.join([str(item) for item in new_sites])
            send_slack_msg(
                context=context,
                msg=f"There were new site_ids when pulling ecocounter data :eco-counter:",
                attachments=[{"text": extra_msg}]
            )
        if len(new_flows) > 0:
            LOGGER.info('New flows added: %s', new_flows)
            extra_msg = '\n'.join([str(item) for item in new_flows])
            send_slack_msg(
                context=context,
                msg=f"There were new flow_ids when pulling ecocounter data :eco-counter:",
                attachments=[{"text": extra_msg}]
            )
        if new_flows == [] and new_sites == []:
            #Mark skipped to visually identify which days added new sites or flows in the UI.
            raise AirflowSkipException('No new sites or flows to add. Marking task as skipped.')

    @task(trigger_rule='none_failed')
    def pull_ecocounter(ds):
        api_conn = BaseHook.get_connection('ecocounter_api_key')
        token = getToken(
            api_conn.host,
            api_conn.login,
            api_conn.password,
            api_conn.extra_dejson['secret_api_hash']
        )
        eco_postgres = PostgresHook("ecocounter_bot")
        
        start_date = dateutil.parser.parse(str(ds))
        end_date = dateutil.parser.parse(str(ds_add(ds, 1)))
        LOGGER.info(f'Pulling data from {start_date} to {end_date}.')

        with eco_postgres.get_conn() as conn:
            for site_id in getKnownSites(conn):
                LOGGER.debug(f'Starting on site {site_id}.')
                for flow_id in getKnownFlows(conn, site_id):
                    LOGGER.debug(f'Starting on flow {flow_id} for site {site_id}.')
                    # empty the count table for this flow
                    truncateFlowSince(flow_id, conn, start_date, end_date)          
                    # and fill it back up!
                    LOGGER.debug(f'Fetching data for flow {flow_id}.')
                    counts = getFlowData(token, flow_id, start_date, end_date)
                    #convert response into a tuple for inserting
                    volume=[]
                    for count in counts:
                        row=(flow_id, count['date'], count['counts'])
                        volume.append(row)
                    insertFlowCounts(conn, volume)
                    LOGGER.debug(f'Data inserted for flow {flow_id} of site {site_id}.')
                LOGGER.info(f'Data inserted for site {site_id}.')
          
    @task_group(
        tooltip="Tasks to check critical data quality measures which could warrant re-running the DAG."
    )
    def data_checks():
        data_check_params = {
            "table": "ecocounter.counts",
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

    check_partitions() >> update_sites_and_flows() >> pull_ecocounter() >> data_checks()

pull_ecocounter_dag()