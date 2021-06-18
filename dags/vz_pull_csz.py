"""
Pipeline for updating the CSZ tracking table with new rows from a periodically-appended master table.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.check_operator import CheckOperator
from airflow.hooks.postgres_hook import PostgresHook
import sys

from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password

    if context.get('task_instance').task_id == 'add_rows':
        task_msg = """TESTING :eyes: :cat_shocked: The Task {task} to add rows to CSZ tracking table failed, 
			<@UHJA7GHQV> please check.""".format(
            task=context.get('task_instance').task_id,)
    
    elif context.get('task_instance').task_id == 'add_geom':
        task_msg = """:cat_shocked: Error adding geoms to CSZ geom table. 
			<@UHJA7GHQV> please check.""".format(
            task=context.get('task_instance').task_id,)
    
    else:
        task_msg = """:blob_fail: The Task {task} found some null CSZ geoms, 
                        <@UHJA7GHQV> please check :eyes:.""".format(
            task=context.get('task_instance').task_id,)    
        
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

# Bot to connect to pgadmin
vz_api_bot = PostgresHook("vz_api_bot")
con = vz_api_bot.get_conn()

DEFAULT_ARGS = {
    'owner': 'cnangini',
    'depends_on_past' : False,
    'email': ['cathy.nangini@toronto.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime(2019, 9, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

# 17 1 * * 1 (execute job at 17 minutes past hour 1 every monday)
dag = DAG('vz_pull_csz', default_args=DEFAULT_ARGS, schedule_interval='@monthly', catchup=False)

# Add rows from the "toadd" table into the "tracking" table if they currently
# do not exist in the tracking table
task1 = PostgresOperator(sql='''
                            INSERT INTO cnangini.test_tracking( tracking_id, 
								ward_no, highway, side, loc,
								prohibited_hours_days, prohibited_months,
								work_order_no, work_order_created,
								work_order_completed)
                            SELECT a.tracking_id, a.ward_no, a.highway, a.side, a.loc,
	                            a.prohibited_hours_days, a.prohibited_months,
	                            a.work_order_no, a.work_order_created, a.work_order_completed
                            FROM   cnangini.test_to_add a
                            LEFT JOIN cnangini.test_tracking t USING (tracking_id)
                            WHERE t.tracking_id IS NULL
                            ORDER BY tracking_id
                            ''',
                            task_id='add_rows',
                            postgres_conn_id='vz_api_bot',
                            autocommit=True,
                            retries = 0,
                            dag=dag)

# Make geoms for the newly added rows and append them to the geom table
task2 = PostgresOperator(sql='''
                            WITH get_ids AS (--find tracking_ids with no geom
	                        SELECT a.tracking_id, a.highway, a.loc
	                        FROM   cnangini.test_tracking a
	                        LEFT JOIN cnangini.test_tracking_geom b ON a.tracking_id = b.tracking_id
	                        WHERE b.geom IS NULL
	                        AND a.tracking_id != 450 --not a real location
	                        ORDER BY tracking_id
                            )
                            , X AS (--make geom for tracking_ids found above
	                        SELECT t.tracking_id,
	                        t.tracking_id AS csz_id,
	                        result.line_geom AS geom
	                        FROM get_ids t,
	                        LATERAL gis.text_to_centreline(tracking_id, highway, loc, NULL)  AS result
                            ),
                            stunion AS (
	                        SELECT tracking_id, csz_id, ST_UNION(geom) as geom
	                        FROM X
	                        GROUP BY tracking_id, csz_id
                            )
                            INSERT INTO cnangini.test_tracking_geom (tracking_id, csz_id, geom)
                            SELECT tracking_id, csz_id, geom
                            FROM stunion
                            ORDER BY tracking_id
                            ''',
                            task_id='add_geom',
                            postgres_conn_id='vz_api_bot',
                            autocommit=True,
                            retries = 0,
                            dag=dag)

# Now check the geom table for any tracking_id that has a NULL geom.
# Airflow will send an alert message if the check returns false (meaning that the
# returned table is not empty).
# NB: Ignore tracking_id = 450 because this location does not exist.
task3 = CheckOperator(
    task_id='check_geom',
    sql='''
        SELECT COUNT(1) = 0
        FROM cnangini.test_tracking a
        LEFT JOIN cnangini.test_tracking_geom b USING(tracking_id)
        WHERE b.geom IS NULL
        AND tracking_id != 450 --not a real location
    ''',
    conn_id='vz_api_bot',
    dag=dag
)

# Task order
task1 >> task2 >> task3
