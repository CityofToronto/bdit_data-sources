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

from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
    from volumes.ecocounter.pull_data_from_api import (
        getToken, getSites, getChannelData, siteIsKnownToUs,
        flowIsKnownToUs, truncateFlowSince, insertFlowCount    
    )
except:
    raise ImportError("Cannot import DAG helper functions.")

DAG_NAME = 'ecocounter_pull'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

API_CONFIG_PATH = '/data/airflow/data_scripts/volumes/ecocounter/config.cfg'

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
    catchup=True,
    params={
        "site_ids": Param(
            default=[0],
            type="array",
            title="An array of site_ids (integers).",
            description="A list of site_ids to pull/aggregate for a single date. Default [0] will pull all flows.",
            items={"type": "number"},
        )
    },
    tags=["ecocounter", "data_pull", "data_checks"],
    doc_md=__doc__
)
def pull_ecocounter_dag():

    @task()
    def pull_ecocounter(ds = None, **context):
        if context["params"]["site_ids"] == [0]:
            SITE_IDS = ()
        else:
            SITE_IDS = tuple(context["params"]["site_ids"])
        
        token = getToken(API_CONFIG_PATH)
        start_time = dateutil.parser.parse(str(ds))
        eco_postgres = PostgresHook("ecocounter_bot")
        unknown_flowids = []
        unknown_siteids = []

        with eco_postgres.get_conn() as conn:
            for site in getSites(token, sites=SITE_IDS):
                
                # only update data for sites / channels in the database
                # but announce unknowns for manual validation if necessary
                if not siteIsKnownToUs(site['id'], conn):
                    print('unknown site', site['id'], site['name'])
                    unknown_siteids.append((site['id'], site['name'], ))
                    continue

                for channel in site['channels']:
                    if not flowIsKnownToUs(channel['id'], conn):
                        print('unknown flow', channel['id'])
                        unknown_flowids.append((channel['id'], channel['name'], ))
                        continue

                    # we do have this site and channel in the database; let's update its counts
                    channel_id = channel['id']
                    print(f'starting on flow {channel_id}')

                    # empty the count table for this flow
                    truncateFlowSince(channel_id, conn, start_time)
            
                    # and fill it back up!
                    print(f'fetching data for flow {channel_id}')
                    counts = getChannelData(token, channel_id, start_time)

                    print(f'inserting data for flow {channel_id}')
                    for count in counts:
                        volume = count['counts']
                        insertFlowCount(channel_id, count['date'], volume, conn)
            
            context["task_instance"].xcom.push(key="unknown_siteids", value=unknown_siteids)
            context["task_instance"].xcom.push(key="unknown_flowids", value=unknown_flowids)
           
    @task(
        retries=0,
        doc_md="""A status message to report any new site or flow ids."""
    )
    def report_missing_ids(**context):
        ti = context["ti"]

        unknown_siteids = ti.xcom_pull(key="unknown_siteids", task_ids="pull_ecocounter")
        unknown_flowids = ti.xcom_pull(key="unknown_flowids", task_ids="pull_ecocounter")

        if unknown_flowids == [] and unknown_siteids == []:
            print("there were no missing ids.")
            exit
        else: #add details of failures to task_fail_slack_alert
            failure_extra_msg = ['One or more site_ids was unknown:', unknown_siteids,
                                 'One or more flow_ids was unknown:', unknown_flowids]
            context.get("task_instance").xcom_push(key="extra_msg", value=failure_extra_msg)
            raise AirflowFailException('One or more tables failed to copy.')

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

    pull_ecocounter() >> report_missing_ids() >> data_checks()

pull_ecocounter_dag()