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
from airflow.sensors.external_task import ExternalTaskSensor

dag_name = 'vds_pull_vdsvehicledata'

#CONNECT TO ITS_CENTRAL
itsc_bot = PostgresHook('itsc_postgres')

#CONNECT TO BIGDATA
vds_bot = PostgresHook('vds_bot')

# Get DAG Owner
dag_owners = Variable.get('dag_owners', deserialize_json=True)
names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)

try:
    from volumes.vds.py.vds_functions import pull_raw_vdsvehicledata, check_vdsvehicledata_partitions
except
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

#this dag deletes any existing data from RDS vds.raw_vdsvehicledata and then pulls and inserts from ITSC
 #then summarizes into length and speed summary tables by 15 minutes.
with DAG(dag_id='vds_pull_vdsvehicledata',
         default_args=default_args,
         max_active_runs=1,
         template_searchpath=os.path.join(repo_path,'volumes/vds/sql'),
         schedule_interval='5 4 * * *') as dag: #daily at 4:05am

    t_upstream_done = ExternalTaskSensor(
        task_id="starting_point",
        external_dag_id="vds_pull_vdsdata",
        external_task_id="update_inventories.done",
        poke_interval=3600, #retry hourly
        mode="reschedule",
        timeout=86400, #one day
        execution_delta=timedelta(minutes=5)
    )

    #this task group checks if all necessary partitions exist and if not executes create functions.
    check_partitions = PythonOperator(
        task_id='check_partitions',
        python_callable=check_vdsvehicledata_partitions,
        op_kwargs = {'rds_conn': vds_bot,
                    'start_date': '{{ ds }}'}
    )  

    #this task group deletes any existing data from `vds.raw_vdsvehicledata` and then pulls and inserts from ITSC into RDS
    with TaskGroup(group_id='pull_vdsvehicledata') as pull_vdsvehicledata:

        #deletes data from vds.raw_vdsvehicledata
        delete_vdsvehicledata_task = PostgresOperator(
            sql="""DELETE FROM vds.raw_vdsvehicledata
                    WHERE
                    dt >= '{{ds}} 00:00:00'::timestamp
                    AND dt < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'""",
            task_id='delete_vdsvehicledata',
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #get vdsvehicledata from ITSC and insert into RDS `vds.raw_vdsvehicledata`
        pull_raw_vdsvehicledata_task = PythonOperator(
            task_id='pull_raw_vdsvehicledata',
            python_callable=pull_raw_vdsvehicledata,
            op_kwargs = {'rds_conn': vds_bot,
                        'itsc_conn': itsc_bot,
                    'start_date': '{{ ds }}'}
        )

        delete_vdsvehicledata_task >> pull_raw_vdsvehicledata_task

    #this task group summarizes vdsvehicledata into `vds.veh_speeds_15min` (5km/h speed bins), `vds.veh_length_15min` (1m length bins)
    with TaskGroup(group_id='summarize_vdsvehicledata') as summarize_vdsvehicledata:
        
        #deletes from and then inserts new data into summary table vds.aggregate_15min_veh_speeds
        summarize_speeds_task = PostgresOperator(
            sql=["delete/delete-veh_speeds_15min.sql", "insert/insert_veh_speeds_15min.sql"],
            task_id='summarize_speeds',
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #deletes from and then insert new data into summary table vds.veh_length_15min
        summarize_lengths_task = PostgresOperator(
            sql=["delete/delete-veh_length_15min.sql", "insert/insert_veh_lengths_15min.sql"],
            task_id='summarize_lengths',
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )
        
        summarize_speeds_task
        summarize_lengths_task

    t_upstream_done >> check_partitions >> pull_vdsvehicledata >> summarize_vdsvehicledata