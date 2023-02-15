from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Variable 

from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2 import connect, Error
import logging

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

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
dag_config = Variable.get('slack_member_id', deserialize_json=True)
list_names = dag_config['raphael'] + ' ' + dag_config['islam'] + ' ' + dag_config['natalie'] 

SLACK_CONN_ID = 'slack_data_pipeline'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    # print this task_msg and tag these users
    task_msg = """The Task {task} failed. A miovision camera may be broken, don't panic. {slack_name} please check. """.format(
        task=context.get('task_instance').task_id, slack_name = list_names,) 
    
    # this adds the error log url at the end of the msg
    slack_msg = task_msg + """ (<{log_url}|log>)""".format(
            log_url=context.get('task_instance').log_url,)
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        )
    return failed_alert.execute(context=context)

default_args = {'owner':'jchew',
                'depends_on_past':False,
                'start_date': datetime(2020, 7, 10),
                'email': ['joven.chew@toronto.ca'],
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }

dag = DAG('check_miovision', default_args=default_args, schedule_interval='0 7 * * *', catchup=False)
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