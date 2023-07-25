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

dag_name = 'vds_pull_vdsvehicledata'

#CONNECT TO ITS_CENTRAL
itsc_bot = PostgresHook('itsc_postgres')

#CONNECT TO BIGDATA
vds_bot = PostgresHook('vds_bot')

# Get DAG Owner
#dag_owners = Variable.get('dag_owners', deserialize_json=True)
#names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    
names = ['gabe']

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0,os.path.join(repo_path,'volumes/vds/py'))
    from vds_functions import task_fail_slack_alert, pull_raw_vdsvehicledata
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
    'on_failure_callback': partial(task_fail_slack_alert, dag_name = dag_name, owners = names),
    'catchup': True,
}

#this dag deletes any existing data from RDS vds.raw_vdsvehicledata and then pulls and inserts from ITSC
 #then summarizes into length and speed summary tables by 15 minutes.
with DAG(dag_id='vds_pull_vdsvehicledata',
         default_args=default_args,
         max_active_runs=5,
         schedule_interval='0 4 * * *') as dag: #daily at 4am

    #this task group deletes any existing data from `vds.raw_vdsvehicledata` and then pulls and inserts from ITSC into RDS
    with TaskGroup(group_id='pull_vdsvehicledata') as pull_vdsvehicledata:

        #deletes data from vds.raw_vdsvehicledata
        delete_vdsvehicledata_task = PostgresOperator(
            sql="""DELETE FROM vds.raw_vdsvehicledata
                    WHERE
                    dt >= '{{ds}} 00:00:00'::timestamp
                    AND dt < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'""",
            task_id='delete_vdsvehicledata',
            dag=dag,
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #get vdsvehicledata from ITSC and insert into RDS `vds.raw_vdsvehicledata`
        pull_raw_vdsvehicledata_task = PythonOperator(
            task_id='pull_raw_vdsvehicledata',
            python_callable=pull_raw_vdsvehicledata,
            dag=dag,
            op_kwargs = {'rds_conn': vds_bot,
                        'itsc_conn': itsc_bot,
                    'start_date': '{{ ds }}'}
        )

        delete_vdsvehicledata_task >> pull_raw_vdsvehicledata_task

    #this task group summarizes vdsvehicledata into `vds.veh_speeds_15min` (5km/h speed bins), `vds.veh_length_15min` (1m length bins)
    with TaskGroup(group_id='summarize_vdsvehicledata') as summarize_vdsvehicledata:
        
        #dlete from vds.veh_speeds_15min prior to inserting
        delete_veh_speed_data = PostgresOperator(
            sql="""DELETE FROM vds.veh_speeds_15min
                    WHERE
                    datetime_15min >= '{{ds}} 00:00:00'::timestamp
                    AND datetime_15min < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'""",
            task_id='delete_veh_speed_data',
            dag=dag,
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #insert new data into summary table vds.aggregate_15min_veh_speeds
        summarize_speeds_task = PostgresOperator(
            sql="SELECT vds.aggregate_15min_veh_speeds('{{ds}} 00:00:00'::timestamp, '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY')",
            task_id='summarize_speeds',
            dag=dag,
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #delete from vds.veh_length_15min prior to inserting
        delete_veh_length_data = PostgresOperator(
            sql="""DELETE FROM vds.veh_length_15min
                    WHERE
                    datetime_15min >= '{{ds}} 00:00:00'::timestamp
                    AND datetime_15min < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'""",
            task_id='delete_veh_length_data',
            dag=dag,
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #insert new data into summary table vds.veh_length_15min
        summarize_lengths_task = PostgresOperator(
            sql="SELECT vds.aggregate_15min_veh_lengths('{{ds}} 00:00:00'::timestamp, '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY')",
            task_id='summarize_lengths',
            dag=dag,
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )
        
        [delete_veh_speed_data >> summarize_speeds_task]
        [delete_veh_length_data >> summarize_lengths_task]

    pull_vdsvehicledata >> summarize_vdsvehicledata