import sys

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator



SLACK_CONN_ID = 'slack_data_pipeline'

def prep_slack_message(message):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_message = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=message,
        username='airflow',
        )
    return slack_message

def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    
    # print this task_msg and tag these users
    task_msg = """As part of End of Year table creation, {task} failed.
        <@U1XGLNWG2> check out the """.format(
        task=context.get('task_instance').task_id,)    
        
    # this adds the error log url at the end of the msg
    slack_msg = task_msg + """<{log_url}|log> :notes_minion: """.format(
            log_url=context.get('task_instance').log_url,)
    failed_alert = prep_slack_message(slack_msg)
    return failed_alert.execute(context=context)

def slack_here_trigger_sql(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    task_instance = context.get('task_instance')
    task_msg = task_instance.xcom_pull(task_ids=task_instance.task_id)
    slack_msg = '''<@U1XGLNWG2>, add the following sql to the :here: TA trigger\n
    ```{task_msg}```'''.format(task_msg=task_msg)
    success_alert = prep_slack_message(slack_msg)
    return success_alert.execute(context=context)

default_args = {'owner':'rdumas',
                'depends_on_past':False,
                'start_date': datetime(2021, 12, 1),
                'email': ['raphael.dumas@toronto.ca'],
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }

here_admin_bot = PostgresHook('here_admin_bot')
bt_bot = PostgresHook('bt_bot')
miovision_bot = PostgresHook('miovision_api_bot')

try:
    sys.path.append('/etc/airflow/data_scripts/here/traffic/')
    from here_eoy_create_tables import create_here_ta_tables, create_sql_for_trigger
except:
    raise ImportError("Cannot import functions for end of year HERE maintenance")

try:
    sys.path.append('/etc/airflow/data_scripts/bluetooth/sql/')
    from bt_eoy_create_tables import create_bt_obs_tables, replace_bt_trigger
except:
    raise ImportError("Cannot import functions for end of year bluetooth maintenance")

try:
    sys.path.append('/etc/airflow/data_scripts/volumes/miovision/sql/')
    from miovision_eoy_create_tables import create_miovision_vol_table, replace_miovision_vol_trigger
except:
    raise ImportError("Cannot import functions for end of year Miovision maintenance")

dag = DAG('eoy_table_create', default_args=default_args,
        schedule_interval='5 9 14-21 12 1') #9:05 on the 3rd Monday of the month

here_create_tables = PythonOperator(task_id='here_create_tables',
                                    python_callable = create_here_ta_tables,
                                    dag = dag,
                                    op_kwargs = {'pg_hook': here_admin_bot,
                                                 'dt': '{{ ds }}'}
                                    )
here_sql_trigger_slack = PythonOperator(task_id='here_sql_trigger_slack',
                                    python_callable = create_sql_for_trigger,
                                    dag = dag,
                                    op_args = ['{{ ds }}'],
                                    on_success_callback=slack_here_trigger_sql)

bt_create_tables = PythonOperator(task_id='bt_create_tables',
                                    python_callable = create_bt_obs_tables,
                                    dag = dag,
                                    op_kwargs = {'pg_hook': bt_bot,
                                                 'dt': '{{ ds }}'}
                                    )
bt_replace_trigger = PythonOperator(task_id='bt_replace_trigger',
                                    python_callable = replace_bt_trigger,
                                    dag = dag,
                                    op_kwargs = {'pg_hook': miovision_bot,
                                                 'dt': '{{ ds }}'})

miovision_create_table = PythonOperator(task_id='miovision_create_table',
                                    python_callable = create_miovision_vol_table,
                                    dag = dag,
                                    op_kwargs = {'pg_hook': miovision_bot,
                                                 'dt': '{{ ds }}'}
                                    )
miovision_replace_trigger = PythonOperator(task_id='miovision_replace_trigger',
                                    python_callable = replace_miovision_vol_trigger,
                                    dag = dag,
                                    op_kwargs = {'pg_hook': miovision_bot,
                                                 'dt': '{{ ds }}'})

here_create_tables >> here_sql_trigger_slack
bt_create_tables >> bt_replace_trigger
miovision_create_table >> miovision_replace_trigger