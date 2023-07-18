import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

#CONNECT TO ITS_CENTRAL
itsc_bot = PostgresHook('itsc_postgres')

#CONNECT TO BIGDATA
vds_bot = PostgresHook('vds_bot')

#op_kwargs:
conns = {'rds_conn': vds_bot, 'itsc_conn': itsc_bot}
start_date = {'start_date': '{{ ds }}'}

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0,os.path.join(repo_path,'volumes/vds/py'))
    from vds_functions import task_fail_slack_alert, pull_raw_vdsdata, pull_detector_inventory, pull_entity_locations
except:
    raise ImportError("Cannot import functions from volumes/vds/py/vds_functions.py.")

dag_name = 'vds_pull_vdsdata'

# Get slack member ids
#dag_owners = Variable.get('dag_owners', deserialize_json=True)
#names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    
names = ['gabe']

SLACK_CONN_ID = 'slack_data_pipeline'
    
default_args = {
    'owner': ','.join(names),
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert,
    'catchup': True,
}

with DAG(dag_name,
         default_args=default_args,
         max_active_runs=5,
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
            dag=dag,
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #get vdsdata from ITSC and insert into RDS `vds.raw_vdsdata`
        pull_raw_vdsdata_task = PythonOperator(
            task_id='pull_raw_vdsdata',
            python_callable=pull_raw_vdsdata,
            dag=dag,
            op_kwargs = conns | start_date 
        )

        delete_raw_vdsdata_task >> pull_raw_vdsdata_task

    #this task group deletes any existing data from RDS vds.volumes_15min and then inserts into the same table
    with TaskGroup(group_id='summarize_v15') as v15data:
        #deletes data from vds.volumes_15min
        delete_v15_task = PostgresOperator(
            sql="""DELETE FROM vds.volumes_15min
                    WHERE
                    datetime_bin >= '{{ds}} 00:00:00'::timestamp
                    AND datetime_bin < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'""",
            task_id='delete_v15',
            dag=dag,
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #inserts summarized data into RDS `vds.volumes_15min`
        summarize_data_task = PostgresOperator(
            sql="SELECT vds.aggregate_15min_vds_volumes('{{ds}} 00:00:00'::timestamp, '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY')",
            task_id='summarize_data',
            dag=dag,
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #deletes data from vds.volumes_15min
        delete_v15_bylane_task = PostgresOperator(
            sql="""DELETE FROM vds.volumes_15min_bylane
                    WHERE
                    datetime_bin >= '{{ds}} 00:00:00'::timestamp
                    AND datetime_bin < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'""",
            task_id='delete_v15_bylane',
            dag=dag,
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #inserts summarized data into RDS `vds.volumes_15min`
        summarize_data_bylane_task = PostgresOperator(
            sql="SELECT vds.aggregate_15min_vds_volumes_bylane('{{ds}} 00:00:00'::timestamp, '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY')",
            task_id='summarize_data_bylane',
            dag=dag,
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )
    
    with TaskGroup(group_id='update_inventories') as update_inventories:
        #get vdsconfig from ITSC and insert into RDS `vds.vdsconfig`
        pull_detector_inventory_task = PythonOperator(
            task_id='pull_and_insert_detector_inventory',
            python_callable=pull_detector_inventory,
            dag=dag,
            op_kwargs = conns
        )

        #get entitylocations from ITSC and insert into RDS `vds.entity_locations`
        pull_entity_locations_task = PythonOperator(
            task_id='pull_and_insert_entitylocations',
            python_callable=pull_entity_locations,
            dag=dag,
            op_kwargs = conns
        )

        pull_detector_inventory_task
        pull_entity_locations_task
        [delete_v15_task >> summarize_data_task]
        [delete_v15_bylane_task >> summarize_data_bylane_task]

    vdsdata >> v15data
    update_inventories