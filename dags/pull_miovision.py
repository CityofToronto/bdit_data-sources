"""
Pipeline to pull miovision daily data and put them into postgres tables using Bash Operator.
Slack notifications is raised when the airflow process fails.
"""
import sys
import os
from functools import partial

import pendulum
from airflow.decorators import dag, task, task_group
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable 
from airflow.providers.postgres.operators.postgres import PostgresOperator

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
except:
    raise ImportError("Cannot import DAG helper functions.")

dag_name = 'pull_miovision'

dag_owners = Variable.get('dag_owners', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

def get_return_value(context):
    """Return records from SQLCheckOperatorWithReturnValue."""
    return_value = context.get("task_instance").xcom_pull(
        task_ids=context.get("task_instance").task_id
    )
    if return_value:
        return return_value
    return ""

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
     # Add 3 hours to ensure that the data are at least 2 hours old
     schedule_interval='0 3 * * *',
     template_searchpath=os.path.join(repo_path,'dags/sql'),
     catchup=False)
def pull_miovision_dag(): 
    #this task group checks if necessary to create new partitions and if so, exexcute.
    @task_group
    def check_partitions():
        YEAR = '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}'
        MONTH = '{{ macros.ds_format(ds, "%Y-%m-%d", "%m") }}'

        @task.short_circuit(ignore_downstream_trigger_rules=False) #only skip immediately downstream task
        def check_annual_partition(ds=None): #check if Jan 1 to trigger partition creates. 
            start_date = datetime.strptime(ds, '%Y-%m-%d')
            if start_date.month == 1 and start_date.day == 1:
                return True
            return False
      
        create_annual_partition = PostgresOperator(
            task_id='create_annual_partitions',
            sql=["SELECT miovision_api.create_yyyy_volumes_partition('volumes', {{ params.year }}::int, 'datetime_bin')",
                 "SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min', {{ params.year }}::int)",
                 "SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt', {{ params.year }}::int)"],
            postgres_conn_id='miovision_api_bot',
            params={"year": YEAR},
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
            sql="SELECT miovision_api.create_mm_nested_volumes_partition('volumes', {{ params.year }}::int, {{ params.month }}::int)",
            postgres_conn_id='miovision_api_bot',
            params={"year": YEAR,
                    "month": MONTH},
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

    @task_group(on_failure_callback = partial(
                    task_fail_slack_alert, extra_msg=get_return_value
                    ))
    def data_checks():
        data_check_params = {
            "table": "miovision_api.volumes_15min_mvt",
            "lookback": '60 days',
            "dt_col": 'datetime_bin',
            "threshold": 0.7
        }
        check_row_count = SQLCheckOperatorWithReturnValue(
            task_id="check_row_count",
            sql="select-row_count_lookback.sql",
            conn_id="miovision_api_bot",
            params=data_check_params | {"col_to_sum": 'volume'},
            retries=2
        )
        check_row_count.doc_md = '''
        Compare the row count today with the average row count from the lookback period.
        '''

        check_distinct_classification_uid = SQLCheckOperatorWithReturnValue(
            task_id="check_distinct_classification_uid",
            sql="select-sensor_id_count_lookback.sql",
            conn_id="miovision_api_bot",
            params=data_check_params | {
                    "id_col": "classification_uid"
                } | {
                    "threshold": 0.999 #dif is floored, so this will catch a dif of 1. 
                },
            retries=2
        )
        check_distinct_classification_uid.doc_md = '''
        Compare the count of classification_uids appearing in today's pull vs the lookback period.
        '''

        check_distinct_intersection_uid = SQLCheckOperatorWithReturnValue(
            task_id="check_distinct_intersection_uid",
            sql="select-sensor_id_count_lookback.sql",
            conn_id="miovision_api_bot",
            params=data_check_params | {
                    "id_col": "intersection_uid"
                } | {
                    "threshold": 0.999 #dif is floored, so this will catch a dif of 1. 
                },
            retries=2
        )
        check_distinct_intersection_uid.doc_md = '''
        Identify intersections which appeared within the lookback period that did not appear today.
        '''

        check_gaps = SQLCheckOperatorWithReturnValue(
            task_id="check_gaps",
            sql="select-find_gaps.sql",
            conn_id="miovision_api_bot",
            end_date=pendulum.datetime(2023, 11, 10, tz="America/Toronto"),
            params={
                "table": "miovision_api.volumes",
                "id_col": 'intersection_uid',
                "dt_col": 'datetime_bin',
                "gap_threshold": '4 hours',
                "default_bin": '1 minute',
            },
            retries=2
        )
        check_gaps.doc_md = '''
        Identify gaps larger than gap_threshold in intersections with values today.
        '''

        check_row_count
        check_distinct_classification_uid
        check_distinct_intersection_uid
        check_gaps

    check_partitions() >> t1 >> data_checks()

pull_miovision_dag()