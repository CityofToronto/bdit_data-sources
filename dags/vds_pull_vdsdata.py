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
from airflow.sensors.external_task import ExternalTaskMarker

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

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)

try:
    from volume.vds.py.vds_functions import pull_raw_vdsdata, pull_detector_inventory, pull_entity_locations, check_vdsdata_partitions
except:
    raise ImportError("Cannot import functions from volumes/vds/py/vds_functions.py.")

try:
    from dags.dag_functions import task_fail_slack_alert
except:
    raise ImportError("Cannot import task_fail_slack_alert.")  
   
default_args = {
    'owner': ','.join(names),
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True, #Allow for progressive longer waits between retries
    'on_failure_callback': partial(task_fail_slack_alert, use_proxy = True),
    'catchup': True,
}

with DAG(dag_name,
         default_args=default_args,
         max_active_runs=1,
         template_searchpath=os.path.join(repo_path,'volumes/vds/sql'),
         schedule_interval='0 4 * * *') as dag: #daily at 4am

    #this task group checks if all necessary partitions exist and if not executes create functions.
    check_partitions = PythonOperator(
        task_id='check_partitions',
        python_callable=check_vdsdata_partitions,
        op_kwargs = {'rds_conn': vds_bot} | start_date 
    )

    #this task group deletes any existing data from RDS vds.raw_vdsdata and then pulls and inserts from ITSC
    with TaskGroup(group_id='pull_vdsdata') as vdsdata:
        #deletes data from vds.raw_vdsdata
        delete_raw_vdsdata_task = PostgresOperator(
            sql="""DELETE FROM vds.raw_vdsdata
                    WHERE
                    dt >= '{{ds}} 00:00:00'::timestamp
                    AND dt < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'""",
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

        #first deletes and then inserts summarized data into RDS `vds.counts_15min`
        summarize_v15_task = PostgresOperator(
            sql=["delete/delete-counts_15min.sql", "insert/insert_counts_15min.sql"],
            task_id='summarize_v15',
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #first deletes and then inserts summarized data into RDS `vds.counts_15min_bylane`
        summarize_v15_bylane_task = PostgresOperator(
            sql=["delete/delete-counts_15min_bylane.sql", "insert/insert_counts_15min_bylane.sql"],
            task_id='summarize_v15_bylane',
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        summarize_v15_task
        summarize_v15_bylane_task

    #this task group pulls the detector inventories
    with TaskGroup(group_id='update_inventories') as update_inventories:
        
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

        t_done = ExternalTaskMarker(
                task_id="done",
                external_dag_id="vds_pull_vdsvehicledata",
                external_task_id="starting_point"
        )

        [pull_detector_inventory_task, pull_entity_locations_task] >> t_done

    update_inventories >> check_partitions >> vdsdata >> v15data #pull then summarize
