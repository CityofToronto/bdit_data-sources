from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2 import connect, Error
import logging
import configparser

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

#To connect to pgadmin
CONFIG = configparser.ConfigParser()
CONFIG.read(r'/home/mohan/cre.cfg')
dbset = CONFIG['DBSETTINGS']
con = connect(**dbset)

def bad_readers(con, run_date):
    with con.cursor() as cursor: 
        select_query = """SELECT reader from mohan.broken_reader()"""
        cursor.execute(select_query)
        while True:
            broken_reader = cursor.fetchall()
            num_readers = len(broken_reader)
            if num_readers > 0:
                LOGGER.info(broken_reader)
                raise Exception (num_readers, ' readers are broken as of', run_date)
            if num_readers == 0:
                LOGGER.info('All readers are working as of date = %s', run_date')
                
SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: A bt Reader may be broken!
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        )
    return failed_alert.execute(context=context)

default_args = {'owner':'mohan',
                'depends_on_past':False,
                'start_date': datetime(2021, 1, 26),
                'email': ['mohanraj.adhikari@toronto.ca'],
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }
dag = DAG('bad_readers',default_args=default_args, schedule_interval='0 17 * * *')

task1 = PythonOperator(
    task_id = 'bad_readers',
    python_callable = bad_readers,
    dag=dag,
    op_kwargs={
      'con': con,
      # execution date is by default a day before if the process runs daily
      'run_date': '{{ ds }}',
    }
)