"""
Pipeline to pull here data every week and put them into the here.ta table using Bash Operator.
Slack notifications is raised when the airflow process fails.
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable 

SLACK_CONN_ID = 'slack_data_pipeline'
dag_config = Variable.get('slack_member_id', deserialize_json=True)
list_names = dag_config['raphael'] + ' ' + dag_config['islam'] + ' ' + dag_config['natalie'] 

def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    # print this task_msg and tag these users
    task_msg = """The Task {task} failed :here: :blob_fail:. {slack_name} please fix it """.format(
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

here_postgres = PostgresHook("here_bot")
rds_con = here_postgres.get_uri()

default_args = {'owner':'rdumas',
                'depends_on_past':False,
                'start_date': datetime(2020, 1, 5),
                'email': ['raphael.dumas@toronto.ca'],
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 3, #Retry 3 times
                'retry_delay': timedelta(minutes=60), #Retry after 60 mins
                'retry_exponential_backoff': True, #Allow for progressive longer waits between retries
                'on_failure_callback': task_fail_slack_alert,
                'env':{'here_bot':rds_con,
                       'LC_ALL':'C.UTF-8', #Necessary for Click
                       'LANG':'C.UTF-8'}
                }

dag = DAG('pull_here',default_args=default_args, schedule_interval=' 30 16 * * * ')
#Every day at 1630

# Execution date seems to be the day before this was run, so yesterday_ds_nodash
# should be equivalent to two days ago. https://stackoverflow.com/a/37739468/4047679

pull_data = BashOperator(
        task_id = 'pull_here',
        bash_command = '/etc/airflow/data_scripts/.venv/bin/python3 /etc/airflow/data_scripts/here/traffic/here_api.py -d /etc/airflow/data_scripts/here/traffic/config.cfg -s {{ yesterday_ds_nodash }} -e {{ yesterday_ds_nodash }} ', 
        retries = 0,
        dag=dag,
        )
