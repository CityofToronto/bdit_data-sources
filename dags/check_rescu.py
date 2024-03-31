import sys
import os
from functools import partial

from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable 

from psycopg2 import sql
import logging
import pendulum

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_functions import task_fail_slack_alert

dag_name = 'rescu_check'

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

#To connect to pgadmin bot
rescu_bot = PostgresHook("rescu_bot")
con = rescu_bot.get_conn()

def check_rescu(con, date_to_pull):
    LOGGER.info('Pulling information for date = %s',date_to_pull)
    with con.cursor() as cur:
        check_raw = sql.SQL('''SELECT COUNT(raw_uid) FROM rescu.raw_15min WHERE dt = {}''').format(sql.Literal(date_to_pull))
        cur.execute(check_raw)
        raw_num = cur.fetchone()[0]
        LOGGER.info('There are %s rows of raw_date for %s', raw_num, date_to_pull)

        check_volume = sql.SQL('''SELECT COUNT(volume_uid) FROM rescu.volumes_15min WHERE datetime_bin::date = {}''').format(sql.Literal(date_to_pull))
        cur.execute(check_volume)
        volume_num = cur.fetchone()[0]
        LOGGER.info('There are %s rows of volume_15min for %s', volume_num, date_to_pull)

        if raw_num == 0 or raw_num < volume_num or volume_num < 7000:
            raise Exception ('There is a PROBLEM here. There is no raw data OR raw_data is less than volume_15min OR volumes_15min is less than 7000 which is way too low')

dag_owners = Variable.get('dag_owners', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2020, 4, 17, tz="America/Toronto"),
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': partial(
                        task_fail_slack_alert,
                        extra_msg="The total volume is too low. Either a lot of loop detectors are down or there's a problem in the pipeline."
                    )
                }

dag = DAG(dag_id = dag_name, default_args=default_args, schedule='0 6 * * *', catchup=False)
# Run at 6 AM local time every day

task1 = PythonOperator(
    task_id = 'check_rescu',
    python_callable = check_rescu,
    dag=dag,
    op_kwargs={
      'con': con,
      # execution date is by default a day before if the process runs daily
      'date_to_pull': '{{ ds }}'
    }
    )
