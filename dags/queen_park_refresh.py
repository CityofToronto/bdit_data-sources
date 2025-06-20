import os
import sys
import logging
import pendulum
from datetime import datetime, timedelta

#from airflow.sdk import dag, task
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert
except ImportError:
    raise ImportError("Cannot import DAG helper functions.")

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

DAG_NAME = 'queens_park_aggregation'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ['Unknown'])

default_args = {'owner': ','.join(DAG_OWNERS),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2022, 9, 27, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

@dag(
    dag_id = DAG_NAME, 
    default_args=default_args, 
    schedule=None, # get triggered by here dag
    catchup=False,
)
def queen_park_refresh():

    ## ShortCircuitOperator Tasks, python_callable returns True or False; False means skip downstream tasks
    @task.short_circuit()
    def check_dow(ds=None):
        execution_date = datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        return execution_date.weekday() == 0

    ## Postgres Tasks
    # Task to aggregate citywide tti daily
    check_weekly = SQLCheckOperator(
        task_id = 'check_weekly',
        conn_id = 'here_bot',
        sql = '''SELECT case when count(distinct dt) = 7
                        THEN TRUE ELSE FALSE 
                        END AS counts
                FROM here.ta
                WHERE
                    dt >= '{{ ds }}'::date - 8
                    AND dt < '{{ ds }}'::date - 1'''
    )

    aggregate_queens_park = SQLExecuteQueryOperator(
        sql='''select data_analysis.generate_queens_park_weekly('{{ macros.ds_add(ds, -8) }}')''',
        task_id='aggregate_queens_park',
        conn_id='here_bot',
        autocommit=True,
        retries = 0
    )
    
    check_dow() >> check_weekly >> aggregate_queens_park

queen_park_refresh()