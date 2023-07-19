import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.macros import ds_add
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

dag_name = 'vds_monitor'

#CONNECT TO ITS_CENTRAL
itsc_bot = PostgresHook('itsc_postgres')

# Get DAG Owner
#dag_owners = Variable.get('dag_owners', deserialize_json=True)
#names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    
names = ['gabe']

#CONNECT TO BIGDATA
vds_bot = PostgresHook('vds_bot')

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0,os.path.join(repo_path,'volumes/vds/py'))
    from vds_functions import monitor_row_counts, task_fail_slack_alert
except:
    raise ImportError("Cannot import functions from volumes/vds/py/vds_functions.py.")
  
def on_success_monitor_log(context):
    print(f"Clearing vds_pull for execution_date `{context.get('task').execution_date}`.")

default_args = {
    'owner': ','.join(names),
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'on_failure_callback': task_fail_slack_alert(dag_name = dag_name, owners = names),
    'catchup': False,
}

#separate dag for monitoring late arrivals so we can use TriggerDagRunOperator
with DAG(dag_name,
         default_args=default_args,
         max_active_runs=1,
         schedule_interval='0 4 * * *'
         ) as dag: #daily at 4am

    for dataset in ('vdsdata', 'vdsvehicledata'):

        #monitors row counts in vdsdata and vdsvehicledata tables
        with TaskGroup(group_id=f"monitor_late_{dataset}") as monitor_row_count:
            
            lookback_days = 30

            #calls the monitoring function (compares rows in ITSC vs RDS databases)
            #branch operator returns list of tasks to trigger (clear_[0-(lookback_days-1)], empty_task)
            monitor = BranchPythonOperator(
                task_id = f"monitor_{dataset}",
                dag=dag,
                python_callable=monitor_row_counts,
                op_kwargs = {
                    'rds_conn': vds_bot,
                    'itsc_conn': itsc_bot,
                    'start_date': '{{ ds }}',
                    'dataset': dataset,
                    'lookback_days': lookback_days
                }
            )
            #empty_task is needed to not cause failure when no backfilling tasks called
            empty_task = EmptyOperator(task_id = "no_backfill", dag=dag)
            
            #create clear tasks, one corresponding to each of the previous (lookback_days) days
            #"airflow tasks clear" to clear existing run and retrigger pull
            for i in range(lookback_days):
                clear_task = TriggerDagRunOperator(
                    task_id=f"clear_{i}",
                    dag=dag,
                    trigger_dag_id=f"vds_pull_{dataset}",
                    reset_dag_run=True,
                    on_success_callback=on_success_monitor_log, #callback listing the execution_date to clear
                    wait_for_completion=False,
                    execution_date='{{macros.ds_add(ds, params.i)}}',
                    params={'i': -i}, #the days are indexed zero through lookback_days starting from start_date (0)
                )
                monitor >> [clear_task, empty_task]
    
    monitor_row_count