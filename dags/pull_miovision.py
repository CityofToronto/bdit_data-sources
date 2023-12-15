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
    from dags.common_tasks import check_jan_1st, check_1st_of_month
except:
    raise ImportError("Cannot import DAG helper functions.")

dag_name = 'pull_miovision'

dag_owners = Variable.get('dag_owners', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

def get_return_value(context) -> str:
    """Return records from SQLCheckOperatorWithReturnValue."""
    task_instance = context.get("task_instance")
    return_value = task_instance.xcom_pull(task_instance.task_id)
    if return_value:
        return return_value
    return ""

default_args = {
    'owner': ','.join(names),
    'depends_on_past':False,
    'start_date': pendulum.datetime(2019, 11, 22, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': partial(
        task_fail_slack_alert, extra_msg=get_return_value
    ),
}

@dag(dag_id=dag_name,
     default_args=default_args,
     # Add 3 hours to ensure that the data are at least 2 hours old
     schedule='0 3 * * *',
     template_searchpath=os.path.join(repo_path,'dags/sql'),
     catchup=False)
def pull_miovision_dag(): 
    #this task group checks if necessary to create new partitions and if so, exexcute.
    @task_group
    def check_partitions():

        create_annual_partition = PostgresOperator(
            task_id='create_annual_partitions',
            sql=["SELECT miovision_api.create_yyyy_volumes_partition('volumes', '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int, 'datetime_bin')",
                 "SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min', '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int)",
                 "SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt', '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int)"],
            postgres_conn_id='miovision_api_bot',
            autocommit=True
        )
      
        create_month_partition = PostgresOperator(
            task_id='create_month_partition',
            sql="""SELECT miovision_api.create_mm_nested_volumes_partitions('volumes'::text, '{{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}'::int, '{{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}'::int)""",
            postgres_conn_id='miovision_api_bot',
            autocommit=True
        )

        check_jan_1st.override(task_id="check_annual_partition")() >> create_annual_partition
        check_1st_of_month.override(task_id="check_month_partition")() >> create_month_partition

    t1 = BashOperator(
        task_id = 'pull_miovision',
        bash_command = '/data/airflow/airflow_venv/bin/python3 /data/airflow/data_scripts/volumes/miovision/api/intersection_tmc.py run-api --path /data/airflow/data_scripts/volumes/miovision/api/config.cfg --dupes --start_date {{ds}} --end_date {{ data_interval_end | ds }} ', 
        retries = 0,
        trigger_rule='none_failed'
    )
    
    @task_group()
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
        )
        check_distinct_intersection_uid.doc_md = '''
        Identify intersections which appeared within the lookback period that did not appear today.
        '''

        check_gaps = SQLCheckOperatorWithReturnValue(
            task_id="check_gaps",
            sql="""SELECT _check, summ, gaps
                FROM public.summarize_gaps_data_check(
                    start_date := '{{ ds }}'::date,
                    end_date := '{{ ds }}'::date,
                    id_col := 'intersection_uid'::text,
                    dt_col := 'datetime_bin'::text,
                    sch_name := 'miovision_api'::text,
                    tbl_name := 'volumes'::text,
                    gap_threshold := '4 hours'::interval,
                    default_bin := '1 minute'::interval,
                    id_col_dtype := null::int
                )""",
            conn_id="miovision_api_bot"
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
