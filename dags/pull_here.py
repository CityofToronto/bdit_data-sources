"""
Pipeline to pull here data every week and put them into the here.ta table using Bash Operator.
Slack notifications is raised when the airflow process fails.
"""
import sys
import os
import pendulum

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable 
from airflow.utils.task_group import TaskGroup

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
except:
    raise ImportError("Cannot import slack alert functions")

dag_name = 'pull_here'

dag_owners = Variable.get('dag_owners', deserialize_json=True)
names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

here_postgres = PostgresHook("here_bot")
rds_con = here_postgres.get_uri()

default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2020, 1, 5, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 3, #Retry 3 times
                'retry_delay': timedelta(minutes=60), #Retry after 60 mins
                'retry_exponential_backoff': True, #Allow for progressive longer waits between retries
                'on_failure_callback': task_fail_slack_alert,
                'env':{'here_bot':rds_con,
                       'LC_ALL':'C.UTF-8', #Necessary for Click
                       'LANG':'C.UTF-8'}
                }

dag = DAG(dag_id = dag_name, default_args = default_args, schedule = ' 30 16 * * * ')
#Every day at 1630

# Execution date seems to be the day before this was run, so yesterday_ds_nodash
# should be equivalent to two days ago. https://stackoverflow.com/a/37739468/4047679

pull_data = BashOperator(
        task_id = 'pull_here',
        bash_command = '/data/airflow/airflow_venv/bin/python3 /data/airflow/data_scripts/here/traffic/here_api.py -d /data/airflow/data_scripts/here/traffic/config.cfg -s {{ yesterday_ds_nodash }} -e {{ yesterday_ds_nodash }} ', 
        dag=dag,
        )

#get dags to trigger from airflow variable 
DAGS_TO_TRIGGER = Variable.get('here_dag_triggers', deserialize_json=True)

# Create a task group for triggering the DAGs
with TaskGroup(group_id='trigger_dags_tasks', dag=dag) as trigger_dags_group:
    # Define TriggerDagRunOperator for each dag to trigger
    trigger_operators = []
    for dag_id in DAGS_TO_TRIGGER:
        trigger_operator = TriggerDagRunOperator(
            task_id=f'trigger_{dag_id}',
            trigger_dag_id=dag_id,
            reset_dag_run = True, # Clear existing dag if already exists (for backfilling)
            dag=dag,
        )
        trigger_operators.append(trigger_operator)

pull_data >> trigger_operators
