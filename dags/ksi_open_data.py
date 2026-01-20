r"""### Daily KSI Open Data Refresh DAG
Pipeline to refresh KSI data for open data and run data checks. 
"""
import os
import sys
import logging
import pendulum
from functools import partial
from datetime import datetime, timedelta

from airflow.sdk import dag, task, task_group
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_owners import owners
    # import custom operators and helper functions
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, send_slack_msg
    from bdit_dag_utils.utils.custom_operators import SQLCheckOperatorWithReturnValue
except:
    raise ImportError("Cannot import DAG helper functions.")

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DAG_NAME = 'ksi_open_data'
DAG_OWNERS = owners.get(DAG_NAME, ["Unknown"])

default_args = {'owner': ','.join(DAG_OWNERS),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2026, 1, 18, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': partial(task_fail_slack_alert, channel="slack_data_pipeline_dev")
                }

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["collision", "open_data"]
)

def ksi_opan_data():
        
    @task_group()
    def data_checks():

        check_deleted_collision = SQLCheckOperatorWithReturnValue(
            task_id="check_deleted_collision",
            sql='''
                    SELECT COUNT(*) = 0 AS _check, 'There are '|| count(*) ||' deleted collision_id comparing to last updated. collision_id: '||
                    ARRAY_TO_STRING(array_agg(collision_id), ', ') AS missing
                    FROM(
                    SELECT collision_id FROM open_data_staging.ksi
                    except all
                    SELECT collision_id FROM open_data.ksi) AS diff
                    ''',
            conn_id="collisions_bot"
        )
        check_dup_collision_person = SQLCheckOperatorWithReturnValue(
            task_id="check_dup_collision",
            sql='''
                    SELECT COUNT(*) = 0 AS _check, 'There are '|| count(*) ||' deleted collision person pair comparing to last updated. collision_id: '||
                    ARRAY_TO_STRING(array_agg(collision_id), ', ') AS missing
                    FROM(
                    SELECT DISTINCT collision_id, per_no FROM open_data_staging.ksi
                    except all
                    SELECT DISTINCT collision_id, per_no FROM open_data.ksi) AS diff
                    ''',
            conn_id="collisions_bot"
        )
        check_deleted_collision >> check_dup_collision_person
    
    refresh_ksi_staging = SQLExecuteQueryOperator(
        sql='''
            REFRESH MATERIALIZED VIEW CONCURRENTLY open_data_staging.ksi;
            ''',
        task_id='refresh_ksi_staging',
        conn_id='collisions_bot',
        autocommit=True,
        retries = 0
    )

    truncate_and_copy = SQLExecuteQueryOperator(
        sql='''
            TRUNCATE open_data.ksi;
            INSERT INTO open_data.ksi 
            SELECT * FROM open_data_staging.ksi;
            ''',
        task_id='truncate_and_copy',
        conn_id='collisions_bot',
        autocommit=True,
        retries = 0
    )

    @task()
    def status_message(**context):
        send_slack_msg(
            context=context,
            msg=f"KSI table successfully refreshed :white_check_mark:. ",
            channel='slack_data_pipeline_dev'
            #use_proxy=True
            )

    data_checks() >> refresh_ksi_staging >> truncate_and_copy >> status_message()

ksi_opan_data()


