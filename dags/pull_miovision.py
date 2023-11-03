"""
Pipeline to pull miovision daily data and put them into postgres tables using Bash Operator.
Slack notifications is raised when the airflow process fails.
"""
import sys
import os
from functools import partial

import pendulum
from airflow.decorators import dag, task_group
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable 

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_functions import task_fail_slack_alert
from dags.custom_operators import SQLCheckOperatorWithReturnValue

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
     schedule_interval='0 3 * * *',
     template_searchpath=os.path.join(repo_path,'dags/sql'),
     catchup=False)
def pull_miovision_dag(): 
# Add 3 hours to ensure that the data are at least 2 hours old

    t1 = BashOperator(
        task_id = 'pull_miovision',
        bash_command = '/etc/airflow/data_scripts/.venv/bin/python3 /etc/airflow/data_scripts/volumes/miovision/api/intersection_tmc.py run-api --path /etc/airflow/data_scripts/volumes/miovision/api/config.cfg --dupes --start_date {{ds}} --end_date {{ data_interval_end | ds }} ', 
        retries = 0,
        trigger_rule='none_failed'
    )

    @task_group(on_failure_callback = partial(
                    task_fail_slack_alert, extra_msg=get_return_value
                    ))
    def data_checks():
        data_check_params = {
            "table": "miovision_api.volumes_15min",
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

        check_row_count
        check_distinct_classification_uid
        check_distinct_intersection_uid

    t1 >> data_checks()

pull_miovision_dag()