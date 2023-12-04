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
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
     schedule='0 3 * * *',
     catchup=False)
def pull_miovision_dag(): 
# Add 3 hours to ensure that the data are at least 2 hours old

    #this task group checks if necessary to create new partitions and if so, exexcute.
    @task_group
    def check_partitions():

        @task.short_circuit(ignore_downstream_trigger_rules=False) #only skip immediately downstream task
        def check_annual_partition(ds=None): #check if Jan 1 to trigger partition creates. 
            start_date = datetime.strptime(ds, '%Y-%m-%d')
            if start_date.month == 1 and start_date.day == 1:
                return True
            return False
      
        create_annual_partition = PostgresOperator(
            task_id='create_annual_partitions',
            sql=["SELECT miovision_api.create_yyyy_volumes_partition('volumes', '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int, 'datetime_bin')",
                 "SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min', '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int)",
                 "SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt', '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int)"],
            postgres_conn_id='miovision_api_bot',
            autocommit=True
        )

        @task.short_circuit(ignore_downstream_trigger_rules=False) #only skip immediately downstream task
        def check_month_partition(ds=None): #check if 1st of Month to trigger partition creates. 
            start_date = datetime.strptime(ds, '%Y-%m-%d')
            if start_date.day == 1:
                return True
            return False
        
        create_month_partition = PostgresOperator(
            task_id='create_month_partition',
            sql="""SELECT miovision_api.create_mm_nested_volumes_partitions('volumes'::text, '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int, '{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}'::int)""",
            postgres_conn_id='miovision_api_bot',
            autocommit=True
        )

        check_annual_partition() >> create_annual_partition
        check_month_partition() >> create_month_partition

    t1 = BashOperator(
        task_id = 'pull_miovision',
        bash_command = '/data/airflow/airflow_venv/bin/python3 /data/airflow/data_scripts/volumes/miovision/api/intersection_tmc.py run-api --path /data/airflow/data_scripts/volumes/miovision/api/config.cfg --dupes --start_date {{ds}} --end_date {{ data_interval_end | ds }} ', 
        retries = 0,
        trigger_rule='none_failed'
    )

    check_partitions() >> t1

pull_miovision_dag()
