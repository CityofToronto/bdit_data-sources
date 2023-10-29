import os
import sys
from airflow import DAG
from airflow.decorators import task
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

from volumes.vds.py.vds_functions import pull_raw_vdsdata, pull_detector_inventory, pull_entity_locations
from dags.dag_functions import task_fail_slack_alert
from dags.custom_operators import SQLCheckOperatorWithReturnValue

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

    #this task group checks if all necessary partitions exist and if not executes create functions.
    with TaskGroup(group_id='check_partitions') as check_partitions_tg:

        @task.short_circuit(ignore_downstream_trigger_rules=False) #only skip immediately downstream task
        def check_partitions(ds=None): #check if Jan 1 to trigger partition creates. 
            start_date = datetime.strptime(ds, '%Y-%m-%d')
            if start_date.month == 1 and start_date.day == 1:
                return True
            return False

        YEAR = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}'
        
        create_partitions = PostgresOperator(
            task_id='create_partitions',
            sql=[#partition by year and month:
                "SELECT vds.partition_vds_yyyymm('raw_vdsdata_div8001', {{ params.year }}::int, 'dt')",
                "SELECT vds.partition_vds_yyyymm('raw_vdsdata_div2', {{ params.year }}::int, 'dt')",
                #partition by year only: 
                "SELECT vds.partition_vds_yyyy('counts_15min_div2', {{ params.year }}::int, 'datetime_15min')",
                "SELECT vds.partition_vds_yyyy('counts_15min_bylane_div2', {{ params.year }}::int, 'datetime_15min')"],
            postgres_conn_id='vds_bot',
            params={"year": YEAR},
            autocommit=True
        )

        check_partitions() >> create_partitions

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
            retries=1,
            trigger_rule='none_failed'
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

    with TaskGroup(group_id='data_checks') as data_checks:
        divisions = [2, 8001]
        for divid in divisions:
            check_avg_rows = SQLCheckOperatorWithReturnValue(
                task_id=f"check_rows_vdsdata_div{divid}",
                sql="select/select-row_count_lookback.sql",
                conn_id='vds_bot',
                params={"table": f'vds.counts_15min_div{divid}',
                        "lookback": '60 days',
                        "dt_col": 'datetime_15min',
                        "col_to_sum": 'num_obs',
                        "threshold": 0.7},
                retries=2,
            )
            check_avg_rows

    [update_inventories, check_partitions_tg] >> vdsdata >> v15data >> data_checks #pull then summarize
