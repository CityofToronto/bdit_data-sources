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

import os
import sys
from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.models import Variable 
from airflow.providers.postgres.operators.postgres import PostgresOperator

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_functions import task_fail_slack_alert

dag_name = "collisions_replicator_transfer"

dag_owners = Variable.get('dag_owners', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2022, 5, 26, tz="America/Toronto"), #start this Thursday, why not?
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }


with DAG(dag_id = dag_name, # going waaaaaayyyyy out on a limb of the magical assumption tree here....
         default_args = default_args,
         schedule='0 3 * * *') as daily_update: #runs at 3am every day
         
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
