from airflow import DAG
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.sensors import ExternalTaskSensor

from psycopg2 import sql
from psycopg2.extras import execute_values
from psycopg2 import connect, Error
import logging

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# Slack alert
SLACK_CONN_ID = 'slack_data_pipeline'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    task_msg = '<@UF4RQFQ11> !!! {task_id} in congestion_refresh DAG failed.'.format(task_id=context.get('task_instance').task_id)   
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

default_args = {'owner':'natalie',
                'depends_on_past':False,
                'start_date': datetime(2020, 1, 5),
                'email': ['natalie.chan@toronto.ca'],
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

dag = DAG('congestion_refresh', 
          default_args=default_args, 
          schedule_interval='30 16 * * * ', # same as pull_here task 
          catchup=False,
)

## Functions
# check if its monday
def is_monday(date_to_pull):	
    execution_date = datetime.strptime(date_to_pull, "%Y-%m-%d")	
    if execution_date.weekday() == 0:		
        return	True
    else: 
        return False

# check if its the start of the month
def is_day_one(date_to_pull):
    execution_date = datetime.strptime(date_to_pull, "%Y-%m-%d")	
    if execution_date.day == 1:		
        return	True
    else: 
        return False

## Tasks ##
## ExternalTaskSensor to wait for pull_here
wait_for_here = ExternalTaskSensor(
    task_id='wait_for_here',
    external_dag_id='pull_here',
    external_task_id='pull_here',
    start_date=datetime(2020, 1, 5),
)

## ShortCircuitOperator Tasks, python_callable returns True or False; False means skip downstream tasks
check_dow = ShortCircuitOperator(
    task_id='check_dow',
    provide_context=False,
    python_callable=is_monday,
    op_kwargs={'date_to_pull': '{{ yesterday_ds }}'},
    dag=dag
    )

check_dom = ShortCircuitOperator(
    task_id='check_dom',
    provide_context=False,
    python_callable=is_day_one,
    op_kwargs={'date_to_pull': '{{ yesterday_ds }}'},
    dag=dag
    )

## Postgres Tasks
# Task to aggregate citywide tti daily
aggregate_citywide_tti = PostgresOperator(sql='''select congestion.generate_citywide_tti_daily('{{ yesterday_ds }}') ''',
                            task_id='aggregate_citywide_tti',
                            postgres_conn_id='natalie',
                            autocommit=True,
                            retries = 0,
                            dag=dag)

# Task to aggregate segment-level tti weekly
aggregate_segments_tti_weekly = PostgresOperator(sql='''select congestion.generate_segments_tti_weekly('{{ yesterday_ds }}')''',
                            task_id='aggregate_segments_tti_weekly',
                            postgres_conn_id='natalie',
                            autocommit=True,
                            retries = 0,
                            dag=dag)


# Task to aggregate segment-level bi monthly
aggregate_segments_bi_monthly = PostgresOperator(sql='''select congestion.generate_segments_bi_monthly('{{ yesterday_ds }}');
                                                        refresh materialized view CONCURRENTLY congestion.citywide_bi_monthly WITH DATA;''',
                            task_id='aggregate_segments_bi_monthly',
                            postgres_conn_id='natalie',
                            autocommit=True,
                            retries = 0,
                            dag=dag)



wait_for_here >> aggregate_citywide_tti >> check_dow >> aggregate_segments_tti_weekly
wait_for_here >> aggregate_citywide_tti >> check_dom >> aggregate_segments_bi_monthly 