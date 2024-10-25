from attr import NOTHING
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.sql import SQLCheckOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Variable
from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2 import connect, Error
import logging
import configparser
import requests
import pendulum

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

#to connect to pgadmin bot
bt_postgres = PostgresHook("bt_bot")

con = bt_postgres.get_conn()

SLACK_CONN_ID = 'slack_data_pipeline'
dag_config = Variable.get('slack_member_id', deserialize_json=True)
list_names = dag_config['raphael'] + ' ' + dag_config['natalie'] 
SLACK_WEBHOOK_URL = BaseHook.get_connection(SLACK_CONN_ID).host + BaseHook.get_connection(SLACK_CONN_ID).password

def format_br_list(returned_list):
# Format broken reader list into a text for slack message.    
    formatted_br = ''
    for i in returned_list:
        br = str(i[0]) + ': ' + i[1] + '\n'
        formatted_br = formatted_br + br 
    return formatted_br

def broken_readers(con, check_date):
# Send slack channel a msg when there are broken readers. 
    with con.cursor() as cursor: 
        sql_query = '''SELECT * from bluetooth.broken_readers_temp(%s::date)'''
        cursor.execute(sql_query, (check_date,))
        broken_readers = cursor.fetchall()
        formatted_br = format_br_list(broken_readers)
        
        if len(broken_readers) == 0:
           pass 

        else: 
        # Send slack msg.
            data = {"text": "The following bluetooth readers are not reporting data as of yesterday:",
                    "username": "Airflow",
                    "channel": 'data_pipelines',
                    "attachments": [{"text": "{}".format(formatted_br)}]}
            r = requests.post(SLACK_WEBHOOK_URL, json=data)


def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    
    task_msg = """:among_us_dead: Error occured in task {task} in Bluetooth DAG. {slack_name} please check.""".format(
                   task=context.get('task_instance').task_id, slack_name = list_names,)

    slack_msg = task_msg + """ (<{log_url}|log>)""".format(log_url=context.get('task_instance').log_url,) 
    
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        )
    return failed_alert.execute(context=context)

default_args = {'owner':'natalie',
                'depends_on_past':False,
                'start_date': pendulum.datetime(2021, 4, 29, tz="America/Toronto"),
                'email': ['mohanraj.adhikari@toronto.ca'],
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

with DAG('temp_bluetooth_check_readers', default_args=default_args, schedule_interval='0 8 * * *', catchup=False) as blip_pipeline:
## Tasks ##
    # Check if the blip data was aggregated into aggr_5min bins as of yesterday.
    pipeline_check = SQLCheckOperator(task_id = 'pipeline_check',
                                      conn_id = 'bt_bot',
                                      sql = '''SELECT  * 
                                               FROM     bluetooth.aggr_5min
					       WHERE    datetime_bin >='{{ ds }}' and datetime_bin < '{{ tomorrow_ds }}' 
						LIMIT 1''' ,
                                      dag = blip_pipeline)   

    # Update bluetooth.routes with the latest last_reported_date
    update_routes_table = PostgresOperator( sql='''SELECT * from bluetooth.insert_report_date_temp()''',
                                            task_id='update_routes_table',
                                            postgres_conn_id='bt_bot',
                                            autocommit=True,
                                            retries = 0,
                                            dag=blip_pipeline
                                            )

    # Update bluetooth.reader_locations with the latest reader status
    update_reader_status = PostgresOperator(sql='''SELECT * from bluetooth.reader_status_history_temp('{{ ds }}')''',
                                            task_id='update_reader_status',
                                            postgres_conn_id='bt_bot',
                                            autocommit=True,
                                            retries = 0,
                                            dag=blip_pipeline
                                            )

    # Send slack channel a msg when there are broken readers 
    broken_readers = PythonOperator(task_id = 'broken_readers',
                                    python_callable = broken_readers,
                                    dag=blip_pipeline,
                                    op_kwargs={ 'con': con,
                                                'check_date': '{{ ds }}'}
                                    ) 

## Flow ##
# Check blip data was aggregated as of yesterday then update routes table and reader status
# Lastly alert slack channel if there are broken readers
pipeline_check >>[update_routes_table, update_reader_status]>> broken_readers

