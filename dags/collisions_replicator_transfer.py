#This script should run after the MOVE dag dumps data into collisions_replicator."ACC"
#Also I heavily cribbed this off of refresh_wys_monthly.py...

# Operators; we need this to operate!
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator 
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
SLACK_CONN_ID = 'slack_data_pipeline'

#This script does things with those operators:
#1) uses ACC to update acc_safe_copy
#2) updates the mat view called collisions_no, which generates ids needed to...
#3) update the events and involved mat views 
#4) throws a sassy slack alert message when it fails

def task_fail_sassy_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    task_msg = 'The {task} in updating collisions failed, <@U02NSCSKFEU> go fix it meow :meow_tableflip: '.format(
            task=context.get('task_instance').task_id,) #K just gotta point out that the last 3 letters in my slack id spell FIRE in French!!!   
        
    slack_msg = task_msg + """(<{log_url}|log>)""".format(
            log_url=context.get('task_instance').log_url,)
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow'
        )
    return failed_alert.execute(context=context)

default_args = {'owner':'scannon',
                'depends_on_past':False,
                'start_date': datetime(2022, 5, 26), #start this Thursday, why not?
                'email': ['sarah.cannon@toronto.ca'],
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_sassy_slack_alert
                }


with DAG('BDITTO_ACC_REPLICATOR', # going waaaaaayyyyy out on a limb of the magical assumption tree here....
         default_args = default_args,
         schedule_interval='0 3 * * *') as daily_update: #runs at 3am every day
         
    update_acc_sc = PostgresOperator(sql = 'SELECT collisions_replicator.update_acc_safe_copy()',
				task_id = 'update_acc_sc',
				postgres_conn_id = 'collisions_bot',
				autocommit = True,
				retries = 0
    )
                                 
    refresh_col_no = PostgresOperator(sql = 'SELECT collisions_replicator.refresh_mat_view_collisions_no()',
				task_id = 'refresh_col_no',
				postgres_conn_id = 'collisions_bot',
				autocommit = True,
				retries = 0
    )
    refresh_events_involved = PostgresOperator(sql = 'SELECT collisions_replicator.refresh_mat_views()',
				task_id = 'refresh_ev_inv_mvs',
				postgres_conn_id = 'collisions_bot',
				autocommit = True,
				retries = 0
    )     
    update_acc_sc >> refresh_col_no >> refresh_events_involved
