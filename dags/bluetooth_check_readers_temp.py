import os
import sys
import logging
import pendulum
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

try:
    # import custom operators and helper functions
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    # pylint: disable=wrong-import-position
    from dags.dag_functions import task_fail_slack_alert, send_slack_msg
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
    # pylint: enable=import-error
except:
    raise ImportError("Cannot import DAG helper functions.")


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

DAG_NAME = 'temp_bluetooth_check_readers'
DAG_OWNERS = Variable.get("dag_owners", deserialize_json=True).get(DAG_NAME, ["Unknown"])

def format_br_list(returned_list):
# Format broken reader list into a text for slack message.    
    formatted_br = ''
    for i in returned_list:
        br = str(i[0]) + ': ' + i[1] + '\n'
        formatted_br = formatted_br + br 
    return formatted_br

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past':False,
    'start_date': pendulum.datetime(2021, 4, 29, tz="America/Toronto"),
    'email': ['mohanraj.adhikari@toronto.ca'],
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval='0 8 * * *',
    catchup=False,
    tags=['bluetooth', 'data_checks']
)
def blip_pipeline():
    ## Tasks ##
    # Check if the blip data was aggregated into aggr_5min bins as of yesterday.
    pipeline_check = SQLCheckOperatorWithReturnValue(
        task_id = 'pipeline_check',
        conn_id = 'bt_bot',
        sql = '''SELECT  * 
                FROM     bluetooth.aggr_5min
                WHERE    datetime_bin >='{{ ds }}' and datetime_bin < '{{ tomorrow_ds }}' 
                LIMIT 1'''
    ) 

    # Update bluetooth.routes with the latest last_reported_date
    update_routes_table = PostgresOperator(
        sql='''SELECT * from bluetooth.insert_report_date_temp()''',
        task_id='update_routes_table',
        postgres_conn_id='bt_bot',
        autocommit=True,
        retries = 0
    )

    # Update bluetooth.reader_locations with the latest reader status
    update_reader_status = PostgresOperator(
        sql='''SELECT * from bluetooth.reader_status_history_temp('{{ ds }}')''',
        task_id='update_reader_status',
        postgres_conn_id='bt_bot',
        autocommit=True,
        retries = 0
    )
    
    # Send slack channel a msg when there are broken readers 
    @task
    def broken_readers(ds, **context):
    # Send slack channel a msg when there are broken readers. 
        bt_postgres = PostgresHook("bt_bot")
        con = bt_postgres.get_conn()

        with con.cursor() as cursor: 
            sql_query = '''SELECT * from bluetooth.broken_readers_temp(%s::date)'''
            cursor.execute(sql_query, (ds,))
            broken_readers = cursor.fetchall()
            formatted_br = format_br_list(broken_readers)
            
            if len(broken_readers) == 0:
                pass 
            else:
                send_slack_msg(
                    context=context,
                    msg="The following bluetooth readers are not reporting data as of yesterday:",
                    attachments=formatted_br
                )

    ## Flow ##
    # Check blip data was aggregated as of yesterday then update routes table and reader status
    # Lastly alert slack channel if there are broken readers
    pipeline_check >> [
        update_routes_table,
        update_reader_status
    ] >> broken_readers()

blip_pipeline()