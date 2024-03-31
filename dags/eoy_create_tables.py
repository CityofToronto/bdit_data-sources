import sys
import os
import pendulum

from airflow import DAG
from datetime import datetime, timedelta
import logging
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import holidays

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from dags.dag_functions import task_fail_slack_alert

dag_name = 'eoy_table_create'

SLACK_CONN_ID = 'slack_data_pipeline'
dag_owners = Variable.get('dag_owners', deserialize_json=True)
slack_ids = Variable.get('slack_member_id', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

list_names = []
for name in names:
    list_names.append(slack_ids.get(name, '@Unknown Slack ID')) #find slack ids w/default = Unkown

def prep_slack_message(message):
    slack_message = SlackWebhookOperator(
        task_id='slack_test',
        slack_webhook_conn_id=SLACK_CONN_ID,
        message=message,
        username='airflow',
        )
    return slack_message

def task_success_slack_alert():   
    # print this task_msg and tag these users
    task_msg = """All End of Year tables have been successfully created, 
        {slack_name} please checkout the tables on the database and make sure 
        they have been properly created. """.format(slack_name = list_names,)   

    return task_msg

def slack_here_trigger_sql(context):
    task_instance = context.get('task_instance')
    task_msg = task_instance.xcom_pull(task_ids=task_instance.task_id)
    slack_msg = '''{slack_name}, add the following sql to the :here: TA trigger\n
    ```{task_msg}```'''.format(task_msg=task_msg, slack_name = list_names,)
    success_alert = prep_slack_message(slack_msg)
    return success_alert.execute(context=context)

def insert_holidays(dt):
    next_year = datetime.strptime(dt, "%Y-%m-%d") + relativedelta(years=1)
    holidays_year = holidays.CA(prov='ON', years=int(next_year.year))
    ref_bot = PostgresHook('ref_bot')
    with ref_bot.get_conn() as con, con.cursor() as cur:
        for dt, name in holidays_year.items():
            name = name.replace('Observed', 'obs')
            cur.execute('INSERT INTO ref.holiday VALUES (%s, %s)', (dt, name))
        
def congestion_create_table(dt):
    next_year = datetime.strptime(dt, "%Y-%m-%d") + relativedelta(years=1)
    year = str(next_year.year)
    congestion_bot = PostgresHook('congestion_bot')
    with congestion_bot.get_conn() as con, con.cursor() as cur:
        cur.execute('SELECT congestion.create_yearly_tables(%s);', (year,))        


default_args = {'owner': ','.join(names), 
                'depends_on_past':False,
                'start_date': pendulum.datetime(2021, 12, 1, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert
                }

here_admin_bot = PostgresHook('here_admin_bot')
bt_bot = PostgresHook('bt_bot')

try:
    sys.path.append('/etc/airflow/data_scripts/here/traffic/')
    from here_eoy_create_tables import create_here_ta_tables
except Exception as exc:
    err_msg = "Error importing functions for end of year HERE maintenance \n" + str(exc)
    raise ImportError(err_msg)
    
try:
    sys.path.append('/etc/airflow/data_scripts/bluetooth/sql/') 
    from bt_eoy_create_tables import create_bt_obs_tables, replace_bt_trigger
except Exception as exc:
    err_msg = "Error importing functions for end of year Bluetooth maintenance \n" + str(exc)
    raise ImportError(err_msg)
    
dag = DAG(dag_id = dag_name, 
          default_args=default_args,
          schedule='5 9 1 12 *') #9:05 on December 1st of every year

here_create_tables = PythonOperator(task_id='here_create_tables',
                                    python_callable = create_here_ta_tables,
                                    dag = dag,
                                    op_kwargs = {'pg_hook': here_admin_bot,
                                                 'dt': '{{ ds }}'}
                                    )

bt_create_tables = PythonOperator(task_id='bt_create_tables',
                                    python_callable = create_bt_obs_tables,
                                    dag = dag,
                                    op_kwargs = {'pg_hook': bt_bot,
                                                 'dt': '{{ ds }}'}
                                    )
bt_replace_trigger = PythonOperator(task_id='bt_replace_trigger',
                                    python_callable = replace_bt_trigger,
                                    dag = dag,
                                    op_kwargs = {'pg_hook': bt_bot,
                                                 'dt': '{{ ds }}'})

insert_holidays = PythonOperator(task_id='insert_holidays',
                                    python_callable = insert_holidays,
                                    dag = dag,
                                    op_args = ['{{ ds }}'])

congestion_create_table = PythonOperator(task_id='congestion_create_table',
                                         python_callable = congestion_create_table,
                                         dag = dag,
                                         op_args = ['{{ ds }}'])

success_alert = SlackWebhookOperator(
                                    task_id='success_msg',
                                    slack_webhook_conn_id=SLACK_CONN_ID,
                                    message=task_success_slack_alert(),
                                    username='airflow',
                                    dag=dag)                                            

here_create_tables >> success_alert
bt_create_tables >> bt_replace_trigger >> success_alert
congestion_create_table >> success_alert
insert_holidays >> success_alert