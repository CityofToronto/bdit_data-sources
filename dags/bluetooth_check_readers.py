from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.operators.sql import SQLCheckOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Variable
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

def broken_readers(con, check_date):
# Send slack channel a msg when there are broken readers. 
    with con.cursor() as cursor: 
        select_query2 = '''SELECT * from bluetooth.broken_readers(%s::date)'''
        cursor.execute(select_query2, (check_date,))
        broken_readers = cursor.fetchall()
        broken_list.append(broken_readers)
        num_broken = len(broken_list)
        #please suggest if this try except works
        try:
            if num_broken == 0:
                pass
            else:
                raise Exception (num_broken, ' readers namely ', broken_list, ' are down.')
        except:
            pass    
           
        
SLACK_CONN_ID = 'slack_data_pipeline'
dag_config = Variable.get('slack_member_id', deserialize_json=True)
list_names = dag_config['raphael'] + ' ' + dag_config['islam'] + ' ' + dag_config['natalie'] 

def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    
    if context.get('task_instance').task_id == 'pipeline_check':
        task_msg = """:among_us_dead: No bluetooth data was found in the database for this date. Remote desktop to the terminal server. Check the blip_api log.""".format(
                task=context.get('task_instance').task_id,)
    else:
        task_msg = """:among_us_dead: Error occured in task {task}. Deep dive required, {slack_name} please check ASAP.""".format(
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
with DAG('bluetooth_check_readers', default_args=default_args, schedule_interval='0 8 * * *', catchup=False) as blip_pipeline:

## Tasks ##
    # Check if the blip data was aggregated into aggr_5min bins as of yesterday.
    pipeline_check = SQLCheckOperator(task_id = 'pipeline_check',
                                      conn_id = 'bt_bot',
                                      sql = '''SELECT   CASE WHEN max(datetime_bin)::date = '{{ ds }}' THEN TRUE 
                                                        ELSE FALSE END 
                                               FROM     bluetooth.aggr_5min''' ,
                                      dag = blip_pipeline)   
                                                          

    # Update bluetooth.routes with the latest last_reported_date
    update_routes_table = PostgresOperator( sql='''SELECT * from bluetooth.insert_report_date()''',
                                            task_id='update_routes_table',
                                            postgres_conn_id='bt_bot',
                                            autocommit=True,
                                            retries = 0,
                                            dag=blip_pipeline
                                            )
    # Update bluetooth.reader_locations with the latest reader status
    update_reader_status = PostgresOperator(sql='''SELECT * from bluetooth.reader_status_history('{{ ds }}')''',
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
