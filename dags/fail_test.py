
from airflow.models import Variable 
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Variable 
from airflow.exceptions import AirflowFailException


def task_to_fail():
    raise AirflowFailException("Failing is my purpose. oh no. oh nooooo.")

dag_config = Variable.get('slack_member_id', deserialize_json=True)
list_names = dag_config['raphael'] + ' ' + dag_config['islam'] + ' ' + dag_config['natalie'] 

SLACK_CONN_ID = 'slack_data_pipeline'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    # print this task_msg and tag these users
    task_msg = """TESTING TESTING, DO NOT PANIC. {slack_name} please ignore. """.format(
                slack_name = list_names,) 
    
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

default_args = {'owner':'natalie',
                'depends_on_past':False,
                'start_date': datetime(2022,1,14),
                'email': ['natalie.chan@toronto.ca'],
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }

dag = DAG('failing_test', default_args=default_args, schedule_interval='0 7 * * *', catchup=False)
# Run at 7 AM local time every day

task1 = PythonOperator(
    task_id = 'failure',
    python_callable = task_to_fail,
    dag=dag
    )