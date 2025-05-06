import os
import sys
import logging
import pendulum
from datetime import timedelta

from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLCheckOperator
from airflow.operators.sql import SQLCheckOperator
from airflow.models import Variable

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
except ImportError:
    raise ImportError("Cannot import DAG helper functions.")

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

DAG_NAME = 'lake_shore_signal_timing_refresh'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ['Unknown'])

default_args = {'owner': ','.join(DAG_OWNERS),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2022, 3, 2, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule='59 18 * * WED', # Every Wednesday at 7 PM, pull here runs at 4:30 PM
    catchup=False,
)
def lake_shore_signal_timing_refresh():
    
    ## Tasks ##
    # SQLSensor to test if all of last week's data is in the database
    # date set as ds - 9 because this task runs on a wednesday and the input should be last monday
    check_here = SQLCheckOperator(
        task_id = 'check_here',
        conn_id='here_bot',
        sql = '''	select case when count(distinct dt) = 7 then TRUE else FALSE end as counts
                from here.ta
                where dt >= '{{ macros.ds_add(ds, -2) }}'::timestamp and 
                        dt < '{{ macros.ds_add(ds, -2) }}'::timestamp + interval '1 week';'''
    )

    check_bt = SQLCheckOperator(
        task_id = 'check_bt',
        conn_id='bt_bot',
        sql = '''select case when count(distinct date(datetime_bin)) = 7 then TRUE else FALSE end as counts
                    from bluetooth.aggr_5min
                    where datetime_bin >= '{{ macros.ds_add(ds, -2) }}'::timestamp AND 
                        datetime_bin < '{{ macros.ds_add(ds, -2) }}'::timestamp + interval '1 week';'''
    )

    ## Postgres Tasks ##
    # Task to aggregate bluetooth data weekly
    aggregate_bt = SQLExecuteQueryOperator(
        sql='''select data_analysis.generate_lakeshore_weekly_bt('{{ macros.ds_add(ds, -2) }}')''',
        task_id='aggregate_bt_weekly',
        conn_id='bt_bot',
        autocommit=True,
        retries = 0
    )

    # Task to aggregate here data weekly
    aggregate_here = SQLExecuteQueryOperator(
        sql='''select data_analysis.generate_lakeshore_weekly_here('{{ macros.ds_add(ds, -2) }}')''',
        task_id='aggregate_here_weekly',
        conn_id='here_bot',
        autocommit=True,
        retries = 0
    )

    ## Flow ##
    # Once check tasks are marked as successful, aggregation task can be scheduled
    check_here >> aggregate_here
    check_bt >> aggregate_bt

lake_shore_signal_timing_refresh()