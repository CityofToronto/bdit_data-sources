import sys
import os

from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable 

import logging
import pendulum

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
except:
    raise ImportError("Cannot import DAG helper functions.")

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

dag_name = 'check_miovision'

#To connect to pgadmin bot
mio_bot = PostgresHook("miovision_api_bot")
con = mio_bot.get_conn()

def check_miovision(con, start_date, end_date):
    date_range = (start_date, end_date)
    LOGGER.info('Check if cameras are working for date range = %s', date_range)
    with con.cursor() as cur: 
        working_machine = '''SELECT miovision_api.determine_working_machine(%s::date, %s::date)'''
        cur.execute(working_machine, date_range)
        LOGGER.info(con.notices[-1]) 
        while True:
            broken_flag = cur.fetchall()
            if not broken_flag: # if broken_flag returns an empty list
                break
            LOGGER.info(broken_flag)
            raise Exception ('A Miovision camera may be broken!')

# Get slack member ids
dag_owners = Variable.get('dag_owners', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    


default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2020, 7, 10, tz="America/Toronto"),
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }

@dag(dag_id=dag_name,
     default_args=default_args,
     schedule='0 7 * * *', # Run at 7 AM local time every day
     catchup=False)
def miovision_check_dag():

    data_check_params = {
        "table": "miovision_api.volumes_15min_mvt",
        "lookback": '60 days',
        "dt_col": 'datetime_bin',
        "threshold": 0.7
    }

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

    check_distinct_intersection_uid
    check_gaps

miovision_check_dag()