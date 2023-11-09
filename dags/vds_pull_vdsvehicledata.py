import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
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

#op_kwargs:
conns = {'rds_conn': vds_bot, 'itsc_conn': itsc_bot}
start_date = {'start_date': '{{ ds }}'}

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)

from volumes.vds.py.vds_functions import pull_raw_vdsvehicledata
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

#this dag deletes any existing data from RDS vds.raw_vdsvehicledata and then pulls and inserts from ITSC
 #then summarizes into length and speed summary tables by 15 minutes.
with DAG(dag_id='vds_pull_vdsvehicledata',
         default_args=default_args,
         max_active_runs=1,
         template_searchpath=[
             os.path.join(repo_path,'volumes/vds/sql'),
             os.path.join(repo_path,'dags/sql')
            ]
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
            sql="SELECT vds.partition_vds_yyyymm('raw_vdsvehicledata', {{ params.year }}::int, 'dt')",
            postgres_conn_id='vds_bot',
            params={"year": YEAR},
            autocommit=True
        )

        check_partitions() >> create_partitions

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
            retries=1,
            trigger_rule='none_failed'
        )

        #get vdsvehicledata from ITSC and insert into RDS `vds.raw_vdsvehicledata`
        pull_raw_vdsvehicledata_task = PythonOperator(
            task_id='pull_raw_vdsvehicledata',
            python_callable=pull_raw_vdsvehicledata,
            op_kwargs = conns | start_date 
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

    with TaskGroup(group_id='data_checks') as data_checks:
        check_avg_rows = SQLCheckOperatorWithReturnValue(
            task_id=f"check_rows_veh_speeds",
            sql="select-row_count_lookback.sql",
            conn_id='vds_bot',
            params={"table": 'vds.veh_speeds_15min',
                    "lookback": '60 days',
                    "dt_col": 'datetime_15min',
                    "col_to_sum": 'count',
                    "threshold": 0.7},
            retries=2
        )
        check_avg_rows

    [t_upstream_done, check_partitions_tg] >> pull_vdsvehicledata >> summarize_vdsvehicledata >> data_checks
