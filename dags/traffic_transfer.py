#This script should run after the MOVE dag dumps data into the "TRAFFIC_NEW" schema...

import pendulum
# Operators; we need this to operate!
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Variable 
SLACK_CONN_ID = 'slack_data_pipeline'

#This script does things with those operators:
#1) does 9 upsert queries to update data in arc_link, arterydata, category, cnt_det, cnt_spd, countinfo, countinfomics, det, node
#2) throws a nattery slack alert message when it fails

dag_name = 'traffic_transfer'

dag_owners = Variable.get('dag_owners', deserialize_json=True)
slack_ids = Variable.get('slack_member_id', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

list_names = []
for name in names:
    list_names.append(slack_ids.get(name, '@Unknown Slack ID')) #find slack ids w/default = Unkown

def task_fail_nattery_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    task_msg = 'The {task} in updating traffic failed, {slack_name} go fix it meow :meow_notlike: '.format(
            task=context.get('task_instance').task_id,
            slack_name = ' '.join(list_names))
        
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

default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2022, 6, 16, tz="America/Toronto"), #start this Thursday, why not?
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_nattery_slack_alert
                }


with DAG(dag_id = dag_name, 
         default_args = default_args,
         schedule_interval='0 2 * * *') as daily_update: #runs at 2am every day
         
    update_arc_link = PostgresOperator(sql = 'SELECT traffic.update_arc_link()',
				task_id = 'update_arc_link',
				postgres_conn_id = 'traffic_bot',
				autocommit = True,
				retries = 0
    )
    
    update_arterydata = PostgresOperator(sql = 'SELECT traffic.update_arterydata()',
				task_id = 'update_arterydata',
				postgres_conn_id = 'traffic_bot',
				autocommit = True,
				retries = 0
    )
    
    update_category = PostgresOperator(sql = 'SELECT traffic.update_category()',
				task_id = 'update_category',
				postgres_conn_id = 'traffic_bot',
				autocommit = True,
				retries = 0
    )
    
    update_cnt_det = PostgresOperator(sql = 'SELECT traffic.update_cnt_det()',
				task_id = 'update_cnt_det',
				postgres_conn_id = 'traffic_bot',
				autocommit = True,
				retries = 0
    )
    
    update_cnt_spd = PostgresOperator(sql = 'SELECT traffic.update_cnt_spd()',
				task_id = 'update_cnt_spd',
				postgres_conn_id = 'traffic_bot',
				autocommit = True,
				retries = 0
    )
    
    update_countinfo = PostgresOperator(sql = 'SELECT traffic.update_countinfo()',
				task_id = 'update_countinfo',
				postgres_conn_id = 'traffic_bot',
				autocommit = True,
				retries = 0
    )
    
    update_countinfomics = PostgresOperator(sql = 'SELECT traffic.update_countinfomics()',
				task_id = 'update_countinfomics',
				postgres_conn_id = 'traffic_bot',
				autocommit = True,
				retries = 0
    )
    
    update_det = PostgresOperator(sql = 'SELECT traffic.update_det()',
				task_id = 'update_det',
				postgres_conn_id = 'traffic_bot',
				autocommit = True,
				retries = 0
    )
    
    update_node = PostgresOperator(sql = 'SELECT traffic.update_node()',
				task_id = 'update_node',
				postgres_conn_id = 'traffic_bot',
				autocommit = True,
				retries = 0
    )
                                   
    update_arc_link >> update_arterydata >> update_category >> update_cnt_det >> update_cnt_spd >> update_countinfo >> update_countinfomics >> update_det >> update_node
