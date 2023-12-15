"""
Pipeline to pull Watch Your Speed sign data data and put them into the wys.raw_data table using Python Operator.
A Slack notification is raised when the airflow process fails.
"""
import os
import sys
from functools import partial
import pendulum

from datetime import timedelta
from airflow.models import Variable
from airflow.decorators import dag, task_group
from airflow.sensors.external_task import ExternalTaskSensor

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
except:
    raise ImportError("Cannot import functions to pull watch your speed data")

dag_name = 'wys_check'

dag_owners = Variable.get('dag_owners', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

default_args = {
    'owner': ','.join(names),
    'depends_on_past':False,
    'start_date': pendulum.datetime(2020, 4, 1, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    #progressive longer waits between retries
    'retry_exponential_backoff': True,
    'on_failure_callback': task_fail_slack_alert
}

@dag(dag_id=dag_name,
     default_args=default_args,
     catchup=False,
     max_active_runs=1,
     template_searchpath=os.path.join(repo_path,'dags/sql'),
     schedule='30 17 * * *' # Run at 5:30 PM local time every day
)
def wys_check_dag():

    t_upstream_done = ExternalTaskSensor(
        task_id="starting_point",
        external_dag_id="pull_wys",
        external_task_id="data_checks.done",
        poke_interval=3600, #retry hourly
        mode="reschedule",
        timeout=86400, #one day
        execution_delta=timedelta(hours=2, minutes=30) #pull_wys scheduled at '0 15 * * *'
    )

    @task_group()
    def data_checks():

        check_distinct_api_id = SQLCheckOperatorWithReturnValue(
            task_id="check_distinct_api_id",
            sql="select-sensor_id_count_lookback.sql",
            conn_id="wys_bot",
            params={
                "table": "wys.speed_counts_agg_5kph",
                "lookback": '60 days',
                "dt_col": 'datetime_bin',
                "id_col": "api_id",
                "threshold": 0.90                
            }
        )

        check_distinct_api_id

    t_upstream_done >> data_checks()
    
wys_check_dag()