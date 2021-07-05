from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
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

#to connect to pgadmin bot
bt_postgres = PostgresHook("bt_bot")

con = bt_postgres.get_conn()
broken_list = []
# check if the blip data was aggregated into aggr_5min bins as of yesterday.
# if the aggr_5min table has at least one record for yesterday, it means pipeline is working 
def pipeline_check(con):
    with con.cursor() as cursor:
        select_query1 = '''SELECT MAX (datetime_bin)::date from bluetooth.aggr_5min'''
        cursor.execute(select_query1)
        latest_date = cursor.fetchone()
        #please suggest if this try except works
        try:
            if (latest_date[0]) >= (datetime.now().date() - timedelta(1)):
                pass
            else:
                raise Exception ('There is no data in bluetooth.aggr_5min for yesterday')
        except:
            pass

def broken_readers(con, check_date):
    with con.cursor() as cursor: 
        select_query2 = '''SELECT * from bluetooth.broken_readers(%s::date)'''
        cursor.execute(select_query2, (check_date,))
        broken_readers = cursor.fetchall()
        broken_list.append(broken_readers)
        num_broken = len(broken_list)
        #please suggest if this try except works
        try:
            if broken_readers[0][0] == '':
                pass
            else:
                raise Exception ('Some readers are down. Please check broken_readers_log')
        except:
            pass    
           
        
SLACK_CONN_ID = 'slack_data_pipeline'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    
    if context.get('task_instance').task_id == 'task1':
        task_msg = """:among_us_dead: Task {task} in blip check update failed, 
            <@U01858E603T> fix it asap.""".format(
                task=context.get('task_instance').task_id,)
    
    elif context.get('task_instance').task_id == 'task5':
        task_msg = """:among_us_ghost: some readers {task} have failed. Check the log, <@U01858E603T>.""".format(
                task=context.get('task_instance').task_id,)
    else:
        task_msg = """:among_us_dead: Error occured in Task {task}. Deep dive required, <@U01858E603T> please check ASAP.""".format(
                task=context.get('task_instance').task_id,)

    slack_msg = task_msg + """ (<{log_url}|log>)""".format(log_url=context.get('task_instance').log_url,) 
    
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
                'start_date': datetime(2021, 4, 29),
                'email': ['mohanraj.adhikari@toronto.ca'],
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }
with DAG('bluetooth_check_readers', default_args=default_args, schedule_interval='0 17 * * *', catchup=False) as blip_pipeline:
    task1 = PythonOperator(
    task_id = 'pipeline_check',
    python_callable = pipeline_check,
    dag=blip_pipeline,
    op_kwargs={
        'con': con
        })
    task2 = PostgresOperator(sql='''SELECT * from bluetooth.insert_report_date()''',
                            task_id='update_routes_table',
                            postgres_conn_id='bt_bot',
                            autocommit=True,
                            retries = 0,
                            dag=blip_pipeline
                            )
    task3 = PostgresOperator(sql='''SELECT * from bluetooth.reader_status_history('{{ ds }}')''',
                            task_id='bt_reader_status_history',
                            postgres_conn_id='bt_bot',
                            autocommit=True,
                            retries = 0,
                            dag=blip_pipeline)
    task4 = PostgresOperator(sql='''SELECT * from bluetooth.reader_locations_dt_update('{{ ds }}')''',
                            task_id='bt_reader_locations',
                            postgres_conn_id='bt_bot',
                            autocommit=True,
                            retries = 0,
                            dag=blip_pipeline)                        
    task5 = PythonOperator(
    task_id = 'broken_readers',
    python_callable = broken_readers,
    dag=blip_pipeline,
    op_kwargs={
        'con': con,
        'check_date': '{{ ds }}'
        }) 
task1, task2, task3, task4, task5