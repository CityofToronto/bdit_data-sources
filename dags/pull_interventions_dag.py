"""
Pipeline to pull CurbTO Intervetions daily and put them into postgres tables using Bash Operator.
Slack notifications is raised when the airflow process fails.
"""
import sys
import os

import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.models import Variable 

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_functions import task_fail_slack_alert

dag_name = 'automate_interventions'

dag_owners = Variable.get('dag_owners', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

default_args = {'owner':names,
                'depends_on_past':False,
                'start_date': pendulum.datetime(2020, 5, 26, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

dag = DAG(dag_id = dag_name, default_args = default_args, schedule = '0 0 * * *')


t1 = BashOperator(
        task_id = 'pull_interventions',
        bash_command = '''/etc/airflow/data_scripts/.venv/bin/python3 /etc/airflow/data_scripts/gis/gccview/pull_interventions.py --mapserver='ActiveTO_and_CurbTO_View' --id=0''', 
        retries = 0,
        dag=dag)