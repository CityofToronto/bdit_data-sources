import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from functools import partial
from airflow.operators.latest_only import LatestOnlyOperator

dag_name = 'vds_pull_vdsdata'

#CONNECT TO ITS_CENTRAL
itsc_bot = PostgresHook('itsc_postgres')

#CONNECT TO BIGDATA
vds_bot = PostgresHook('vds_bot')

# Get DAG Owner
dag_owners = Variable.get('dag_owners', deserialize_json=True)
names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

#op_kwargs:
conns = {'rds_conn': vds_bot, 'itsc_conn': itsc_bot}
start_date = {'start_date': '{{ ds }}'}

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0,os.path.join(repo_path,'volumes/vds/py'))
    from vds_functions import task_fail_slack_alert, pull_raw_vdsdata, pull_detector_inventory, pull_entity_locations
except:
    raise ImportError("Cannot import functions from volumes/vds/py/vds_functions.py.")
   
default_args = {
    'owner': ','.join(names),
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True, #Allow for progressive longer waits between retries
    'on_failure_callback': partial(task_fail_slack_alert, owners = names),
    'catchup': True,
}

with DAG(dag_name,
         default_args=default_args,
         max_active_runs=5,
         template_searchpath=os.path.join(repo_path,'volumes/vds/sql'),
         schedule_interval='0 4 * * *') as dag: #daily at 4am

    #this task group deletes any existing data from RDS vds.raw_vdsdata and then pulls and inserts from ITSC
    with TaskGroup(group_id='pull_vdsdata') as vdsdata:
        #deletes data from vds.raw_vdsdata
        delete_raw_vdsdata_task = PostgresOperator(
            sql="""DELETE FROM vds.raw_vdsdata
                    WHERE
                    datetime_15min >= '{{ds}} 00:00:00'::timestamp
                    AND datetime_15min < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'""",
            task_id='delete_vdsdata',
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #get vdsdata from ITSC and insert into RDS `vds.raw_vdsdata`
        pull_raw_vdsdata_task = PythonOperator(
            task_id='pull_raw_vdsdata',
            python_callable=pull_raw_vdsdata,
            op_kwargs = conns | start_date 
        )

        delete_raw_vdsdata_task >> pull_raw_vdsdata_task

    #this task group deletes any existing data from RDS summary tables (vds.volumes_15min, vds.volumes_15min_bylane) and then inserts into the same table
    with TaskGroup(group_id='summarize_v15') as v15data:
        #deletes data from vds.counts_15min
        delete_v15_task = PostgresOperator(
            sql="""DELETE FROM vds.counts_15min
                    WHERE
                    datetime_15min >= '{{ds}} 00:00:00'::timestamp
                    AND datetime_15min < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'""",
            task_id='delete_v15',
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #inserts summarized data into RDS `vds.counts_15min`
        summarize_v15_task = PostgresOperator(
            sql="insert/insert_counts_15min.sql",
            task_id='summarize_v15',
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #deletes data from vds.counts_15min_bylane
        delete_v15_bylane_task = PostgresOperator(
            sql="""DELETE FROM vds.counts_15min_bylane
                    WHERE
                    datetime_15min >= '{{ds}} 00:00:00'::timestamp
                    AND datetime_15min < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'""",
            task_id='delete_v15_bylane',
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #inserts summarized data into RDS `vds.counts_15min_bylane`
        summarize_v15_bylane_task = PostgresOperator(
            sql="insert/insert_counts_15min_bylane.sql",
            task_id='summarize_v15_bylane',
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        [delete_v15_task >> summarize_v15_task]
        [delete_v15_bylane_task >> summarize_v15_bylane_task]

    #this task group pulls the detector inventories
    with TaskGroup(group_id='update_inventories') as update_inventories:
        
        #only run update inventories for latest scheduled interval
            # (ie. skip during catchup)
        skip_update_inventories = LatestOnlyOperator(
            task_id = 'skip_update_inventories'
        )

        #get vdsconfig from ITSC and insert into RDS `vds.vdsconfig`
        pull_detector_inventory_task = PythonOperator(
            task_id='pull_and_insert_detector_inventory',
            python_callable=pull_detector_inventory,
            op_kwargs = conns
        )

        #get entitylocations from ITSC and insert into RDS `vds.entity_locations`
        pull_entity_locations_task = PythonOperator(
            task_id='pull_and_insert_entitylocations',
            python_callable=pull_entity_locations,
            op_kwargs = conns
        )

        skip_update_inventories >> [pull_detector_inventory_task, pull_entity_locations_task]

    vdsdata >> v15data #pull then summarize
    update_inventories