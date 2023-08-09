#This script should run after the MOVE dag dumps data into collisions_replicator."ACC"
#Also I heavily cribbed this off of refresh_wys_monthly.py...

#This script does things with those operators:
#1) uses ACC to update acc_safe_copy
#2) updates the mat view called collisions_no, which generates ids needed to...
#3) updates the events_cursed and involved_cursed mat views
#4) updates the MOVE "raw" events and involved mat views
#5) updates the MOVE "norm" events and involved mat views
#6) updates the MOVE involved mat view
#7) updates the MOVE events mat view
#8) throws a sassy slack alert message when it fails

# Operators; we need this to operate!

from datetime import datetime, timedelta
import os
import sys
from threading import local
import psycopg2
from psycopg2 import sql
import requests
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable 
from dateutil.parser import parse
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

dag_name = 'collisions_replicator_transfer'

dag_owners = Variable.get('dag_owners', deserialize_json=True)
slack_ids = Variable.get('slack_member_id', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

list_names = []
for name in names:
    list_names.append(slack_ids.get(name, '@Unknown Slack ID')) #find slack ids w/default = Unkown


SLACK_CONN_ID = 'slack_data_pipeline'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed / Tâche échouée.
            {slack_name} please check.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
            slack_name=' '.join(list_names),
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



default_args = {'owner':','.join(names),
                'depends_on_past':False,
                'start_date': datetime(2022, 5, 26), #start this Thursday, why not?
                'email': ['sarah.cannon@toronto.ca'],
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }


with DAG('collisions_replicator_transfer', # going waaaaaayyyyy out on a limb of the magical assumption tree here....
         default_args = default_args,
         schedule_interval='0 3 * * *') as daily_update: #runs at 3am every day
         
    update_acc_sc = PostgresOperator(sql = 'SELECT collisions_replicator.update_acc_safe_copy()',
				task_id = 'update_acc_sc',
				postgres_conn_id = 'collisions_bot',
				autocommit = True,
				retries = 0
    )
    refresh_raw_move_matviews = PostgresOperator(sql = 'SELECT collisions_replicator.refresh_raw_fields_mv()',
				task_id = 'refresh_raw_move_matviews',
				postgres_conn_id = 'collisions_bot',
				autocommit = True,
				retries = 0
    )
    
    refresh_norm_move_matviews = PostgresOperator(sql = 'SELECT collisions_replicator.refresh_norm_mv()',
				task_id = 'refresh_norm_move_matviews',
				postgres_conn_id = 'collisions_bot',
				autocommit = True,
				retries = 0
    )
    
    refresh_inv_move_matview = PostgresOperator(sql = 'SELECT collisions_replicator.refresh_inv_mv()',
				task_id = 'refresh_inv_move_matview',
				postgres_conn_id = 'collisions_bot',
				autocommit = True,
				retries = 0
    )
    
    refresh_ev_move_matview = PostgresOperator(sql = 'SELECT collisions_replicator.refresh_ev_mv()',
				task_id = 'refresh_ev_move_matview',
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
    
    update_acc_sc >> refresh_raw_move_matviews >> refresh_norm_move_matviews >> refresh_inv_move_matview >> refresh_ev_move_matview >> refresh_col_no >> refresh_events_involved
