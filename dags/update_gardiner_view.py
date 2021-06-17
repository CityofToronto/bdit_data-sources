from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

heroku_conn = BaseHook.get_connection("heroku_token")
heroku_token = heroku_conn.password

heroku_postgres = PostgresHook("heroku_postgres")
heroku_con = heroku_postgres.get_uri()

heroku_bot = PostgresHook("heroku_bot_rds")
rds_con = heroku_bot.get_uri()

SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed, oh no!. 
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
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return failed_alert.execute(context=context)


default_args = {
 'owner': 'natalie',
 'depends_on_past': False,
 'start_date': datetime(2019, 12, 12),
 'email_on_failure': False, 
 'email': ['natalie.chan@toronto.ca'], 
 'retries': 0,
 'on_failure_callback': task_fail_slack_alert,
 'env':{'HEROKU_TOKEN':heroku_token,
                      'heroku_string':heroku_con,
                      'heroku_rds':rds_con}
}

dag = DAG('update_gardiner_view', 
 schedule_interval='30 7 * * 1-7', 
 default_args=default_args 
 ) 

# Update materialized view  
update_daily = BashOperator(
    dag=dag,
    task_id = 'update_daily',
    bash_command = '''set -o pipefail; psql $heroku_rds -v "ON_ERROR_STOP=1" -c "REFRESH MATERIALIZED VIEW data_analysis.gardiner_dash_daily_mat;"''',
    retries = 0
    )   

update_baseline = BashOperator(
    dag=dag,
    task_id = 'update_baseline',
    bash_command = '''set -o pipefail; psql $heroku_rds -v "ON_ERROR_STOP=1" -c "REFRESH MATERIALIZED VIEW data_analysis.gardiner_dash_baseline_mat;"''',
    retries = 0
    )

reload_gunicorn = BashOperator(
	dag=dag,
	task_id= 'reload_gunicorn',
	bash_command = '''killall 'gunicorn: master [gardiner]' -HUP''',
	retries=0
)

[update_daily, update_baseline] >> reload_gunicorn