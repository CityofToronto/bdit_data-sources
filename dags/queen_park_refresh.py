import os
import sys
import logging
import pendulum
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
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

    # check if its monday
    def is_monday(date_to_pull):
        execution_date = datetime.strptime(date_to_pull, "%Y-%m-%d")
        return execution_date.weekday() == 0

    ## ShortCircuitOperator Tasks, python_callable returns True or False; False means skip downstream tasks
    check_dow = ShortCircuitOperator(
        task_id='check_dow',
        python_callable=is_monday,
        op_kwargs={'date_to_pull': '{{ macros.ds_add(ds, -1) }}'}
    )

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
                    dt >= '{{ macros.ds_add(ds, -8) }}'::date
                    AND dt < '{{ macros.ds_add(ds, -1) }}'::date'''
    )

    aggregate_queens_park = SQLExecuteQueryOperator(
        sql='''select data_analysis.generate_queens_park_weekly('{{ macros.ds_add(ds, -8) }}') ''',
        task_id='aggregate_queens_park',
        conn_id='here_bot',
        autocommit=True,
        retries = 0
    )
    
    check_dow >> check_weekly >> aggregate_queens_park

queen_park_refresh()