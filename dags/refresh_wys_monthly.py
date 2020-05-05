"""
Pipeline to pull Watch Your Speed sign data data and put them into the wys.raw_data table using Python Operator.
A Slack notification is raised when the airflow process fails.
"""
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from dateutil.relativedelta import relativedelta
SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: WYS Api Pull Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow'
    )
    return failed_alert.execute(context=context)

default_args = {'owner':'rdumas',
                'depends_on_past':False,
                'start_date': datetime(2020, 4, 30),
                'email': ['raphael.dumas@toronto.ca'],
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }


mon = datetime.today().date() - relativedelta(months=1)
mon = mon.replace(day=1)

with DAG('wys_monthly_summary',default_args=default_args,
         schedule_interval='0 12 1 * *') as monthly_summary:
    wys_views = PostgresOperator(sql='SELECT wys.refresh_mat_views()',
                            task_id='wys_views',
                            postgres_conn_id='wys_bot',
                            autocommit=True,
                            retries = 0,
                            dag=monthly_summary)
    od_wys_view = PostgresOperator(sql='SELECT wys.refresh_od_mat_view()',
                            task_id='od_wys_view',
                            postgres_conn_id='wys_bot',
                            autocommit=True,
                            retries = 0,
                            dag=monthly_summary)
    wys_mobile_summary = PostgresOperator(sql='SELECT wys.mobile_summary_for_month(%s)',
                            task_id='wys_mobile_summary',
                            postgres_conn_id='wys_bot',
                            parameters=(mon,),
                            autocommit=True,
                            retries = 0,
                            dag=monthly_summary)
    wys_stat_summary = PostgresOperator(sql='SELECT wys.stationary_summary_for_month(%s)',
                            task_id='wys_stat_summary',
                            postgres_conn_id='wys_bot',
                            parameters=(mon,),
                            autocommit=True,
                            retries = 0,
                            dag=monthly_summary)
    wys_views >> [wys_mobile_summary, wys_stat_summary, od_wys_view]
