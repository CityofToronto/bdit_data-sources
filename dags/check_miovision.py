"""
Pipeline to run SQL data quality check on daily Miovision data pull.
Uses `miovision_api.determine_working_machine` to check if there are gaps of 4 hours or more for any camera.
Deprecated by `miovision_check` DAG. 
"""
import sys
import os

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable 

import logging
import pendulum

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_functions import task_fail_slack_alert

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
                'end_date': pendulum.datetime(2023, 12, 21, tz="America/Toronto"),
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }

dag = DAG(dag_id = dag_name,
          default_args=default_args,
          schedule='0 7 * * *',
          catchup=False,
          tags = ['miovision', "data_checks", "archived"],
          doc_md=__doc__
)
# Run at 7 AM local time every day

task1 = PythonOperator(
    task_id = 'check_miovision',
    python_callable = check_miovision,
    dag=dag,
    op_kwargs={
      'con': con,
      # execution date is by default a day before if the process runs daily
      'start_date': '{{ ds }}', 
      'end_date' : '{{ macros.ds_add(ds, 1) }}'
    }
)