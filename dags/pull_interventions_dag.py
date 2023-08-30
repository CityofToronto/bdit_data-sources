"""
Pipeline to pull CurbTO Intervetions daily and put them into postgres tables using Bash Operator.
Slack notifications is raised when the airflow process fails.
"""

import pendulum
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Variable 

dag_name = 'automate_interventions'

SLACK_CONN_ID = 'slack_data_pipeline'
dag_owners = Variable.get('dag_owners', deserialize_json=True)
slack_ids = Variable.get('slack_member_id', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

list_names = []
for name in names:
    list_names.append(slack_ids.get(name, '@Unknown Slack ID')) #find slack ids w/default = Unkown

def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    # print this task_msg and tag these users
    task_msg = """The Task {task} failed :meow_dio: {slack_name} please fix it """.format(
        task=context.get('task_instance').task_id, 
        slack_name = ' '.join(list_names),) 
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

default_args = {'owner':names,
                'depends_on_past':False,
                'start_date': pendulum.datetime(2020, 5, 26, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

dag = DAG(dag_id = dag_name, default_args = default_args, schedule_interval = '0 0 * * *')


t1 = BashOperator(
        task_id = 'pull_interventions',
        bash_command = '''/etc/airflow/data_scripts/.venv/bin/python3 /etc/airflow/data_scripts/gis/gccview/pull_interventions.py --mapserver='ActiveTO_and_CurbTO_View' --id=0''', 
        retries = 0,
        dag=dag)