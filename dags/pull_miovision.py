"""
Pipeline to pull miovision daily data and put them into postgres tables using Bash Operator.
Slack notifications is raised when the airflow process fails.
"""
import sys
import os

import pendulum
from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable 

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_functions import task_fail_slack_alert

dag_name = 'pull_miovision'

dag_owners = Variable.get('dag_owners', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2019, 11, 22, tz="America/Toronto"),
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }

@dag(dag_id=dag_name,
     default_args=default_args,
     schedule_interval='0 3 * * *',
     catchup=False)
def pull_miovision_dag(): 
# Add 3 hours to ensure that the data are at least 2 hours old

    t1 = BashOperator(
        task_id = 'pull_miovision',
        bash_command = '/etc/airflow/data_scripts/.venv/bin/python3 /etc/airflow/data_scripts/volumes/miovision/api/intersection_tmc.py run-api --path /etc/airflow/data_scripts/volumes/miovision/api/config.cfg --dupes --start_date {{ds}} --end_date {{ data_interval_end | ds }} ', 
        retries = 0,
        trigger_rule='none_failed'
    )

    t1

pull_miovision_dag()