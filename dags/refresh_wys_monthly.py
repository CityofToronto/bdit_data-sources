"""
Refresh WYS Materialized Views and run monthly aggregation function for Open Data.
A Slack notification is raised when the airflow process fails.
"""
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable 
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from dateutil.relativedelta import relativedelta

dag_name = 'wys_monthly_summary'

SLACK_CONN_ID = 'slack_data_pipeline'
dag_owners = Variable.get('dag_owners', deserialize_json=True)
slack_ids = Variable.get('slack_member_id', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

list_names = []
for name in names:
    list_names.append(slack_ids.get(name, '@Unknown Slack ID')) #find slack ids w/default = Unkown

def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    task_msg = 'The {task} in Refreshing the WYS Open Data failed, {slack_name} go fix it meow :meow_headache: '.format(
            task=context.get('task_instance').task_id, slack_name = ' '.join(list_names),)    
        
    slack_msg = task_msg + """(<{log_url}|log>)""".format(
            log_url=context.get('task_instance').log_url,)
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        )
    return failed_alert.execute(context=context)

default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': datetime(2020, 4, 30),
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }

def last_month(ds):
    dt = datetime.strptime(ds, "%Y-%m-%d")
    # Don't need to add `months=1` because that seems to be taken care of with 
    # the monthly scheduling 
    return (dt - relativedelta(day=1)).strftime("%Y-%m-%d")

with DAG(dag_id = dag_name,
         default_args=default_args,
         user_defined_macros={
            'last_month' : last_month
          },
         schedule_interval='0 3 2 * *') as monthly_summary:
    wys_view_stat_signs = PostgresOperator(
                            #sql in bdit_data-sources/wys/api/sql/mat-view-stationary-signs.sql
                            sql='SELECT wys.refresh_mat_view_stationary_signs()',
                            task_id='wys_view_stat_signs',
                            postgres_conn_id='wys_bot',
                            autocommit=True,
                            retries = 0,
                            dag=monthly_summary)
    wys_view_mobile_api_id = PostgresOperator(
                            #sql in bdit_data-sources/wys/api/sql/function-refresh_mat_view_mobile_api_id.sql
                            #sql in bdit_data-sources/wys/api/sql/create-view-mobile_api_id.sql
                            sql='SELECT wys.refresh_mat_view_mobile_api_id()', 
                            task_id='wys_view_mobile_api_id',
                            postgres_conn_id='wys_bot',
                            autocommit=True,
                            retries = 0,
                            dag=monthly_summary)
    od_wys_view = PostgresOperator(
                            #sql in bdit_data-sources/wys/api/sql/open_data/mat-view-stationary-locations.sql
                            sql='SELECT wys.refresh_od_mat_view()',
                            task_id='od_wys_view',
                            postgres_conn_id='wys_bot',
                            autocommit=True,
                            retries = 0,
                            dag=monthly_summary)
    wys_mobile_summary = PostgresOperator(
                            #sql in bdit_data-sources/wys/api/sql/function-mobile-summary.sql
                            sql="SELECT wys.mobile_summary_for_month('{{ last_month(ds) }}')",
                            task_id='wys_mobile_summary',
                            postgres_conn_id='wys_bot',
                            autocommit=True,
                            retries = 0,
                            dag=monthly_summary)
    wys_stat_summary = PostgresOperator(
                            #sql in bdit_data-sources/wys/api/sql/function-stationary-sign-summary.sql
                            sql="SELECT wys.stationary_summary_for_month('{{ last_month(ds) }}')", 
                            task_id='wys_stat_summary',
                            postgres_conn_id='wys_bot',
                            autocommit=True,
                            retries = 0,
                            dag=monthly_summary)

    # Stationary signs
    wys_view_stat_signs >> [wys_stat_summary, od_wys_view]
    # Mobile signs
    wys_view_mobile_api_id >> wys_mobile_summary