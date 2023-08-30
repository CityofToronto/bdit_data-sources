"""
Pipeline for pulling two vz google sheets data and putting them into postgres tables using Python Operator.
"""
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable 
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from googleapiclient.discovery import build
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Variable
import os
import sys

dag_name = 'vz_google_sheets'

SLACK_CONN_ID = 'slack_data_pipeline'
dag_owners = Variable.get('dag_owners', deserialize_json=True)
slack_ids = Variable.get('slack_member_id', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

list_names = []
for name in names:
    list_names.append(slack_ids.get(name, '@Unknown Slack ID')) #find slack ids w/default = Unkown

dag_config = Variable.get('ssz_spreadsheet_ids', deserialize_json=True)
ssz2018 = dag_config['ssz2018']
ssz2019 = dag_config['ssz2019']
ssz2020 = dag_config['ssz2020']
ssz2021 = dag_config['ssz2021']
ssz2022 = dag_config['ssz2022']

"""The following defines the details of the spreadsheets read and details of the table used to store the data. They are put into a dict based on year. 
The range for both sheets is set from the beginning up to line 180 to include rows of schools which might be added later on.
Details of the spreadsheets are ID and range whereas details of the table are name of schema and table.
The ID is the value between the "/d/" and the "/edit" in the URL of the spreadsheet.
"""
sheets = {
           2018: {'spreadsheet_id' : ssz2018, 
                  'range_name' : 'Master List!A4:AC180',
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2018_raw'},
           2019: {'spreadsheet_id' : ssz2019, 
                  'range_name' : '2019 Master List!A3:AC180', 
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2019_raw'},
           2020: {'spreadsheet_id' : ssz2020, 
                  'range_name' : 'Master Sheet!A3:AC180', 
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2020_raw'},
           2021: {'spreadsheet_id' : ssz2021, 
                  'range_name' : 'Master Sheet!A3:AC180', 
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2021_raw'},
           2022: {'spreadsheet_id' : ssz2022,
                  'range_name' : 'Master Sheet!A3:AC180',
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2022_raw'}
         }

def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    # print this task_msg and tag these users
    invalid_rows = context.get('task_instance').xcom_pull(task_ids=context.get('task_instance').task_id, 
                                                            key='invalid_rows')
    if invalid_rows:
        task_msg = """The Task vz_google_sheets (ssz):{task} failed.
                    Found one or more invalid records. {slack_name} please email David Tang """.format(
            task=context.get('task_instance').task_id, 
            slack_name = ' '.join(list_names),) 
    else:
        task_msg = """The Task vz_google_sheets (ssz):{task} failed. {slack_name} please fix it """.format(
            task=context.get('task_instance').task_id, 
            slack_name = ' '.join(list_names),) 
    
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

#to read the python script for pulling data from google sheet and putting it into tables in postgres
try:
    # absolute path to the repo
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    # path of the python scripts
    sys.path.insert(0,os.path.join(repo_path,'gis/school_safety_zones'))
    from schools import pull_from_sheet
except:
    raise ImportError("Cannot import functions to pull school safety zone list")

#to get credentials to access google sheets
vz_api_hook = GoogleCloudBaseHook('vz_api_google')
cred = vz_api_hook.get_credentials()
service = build('sheets', 'v4', credentials=cred, cache_discovery=False)

#To connect to pgadmin bot
vz_api_bot = PostgresHook("vz_api_bot")
con = vz_api_bot.get_conn()

DEFAULT_ARGS = {
    'owner': ','.join(names),
    'depends_on_past' : False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': pendulum.datetime(2019, 9, 30, tz="America/Toronto"),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context':True,
    'on_failure_callback': task_fail_slack_alert
}

dag = DAG(dag_id = dag_name, default_args = DEFAULT_ARGS, schedule_interval = '@daily', catchup = False)

task1 = PythonOperator(
    task_id='2018',
    python_callable=pull_from_sheet,
    dag=dag,
    op_args=[con, service, 2018, sheets[2018]]
    )
 
task2 = PythonOperator(
    task_id='2019',
    python_callable=pull_from_sheet,
    dag=dag,
    op_args=[con, service, 2019, sheets[2019]]
    )
     
task3 = PythonOperator(
    task_id='2020',
    python_callable=pull_from_sheet,
    dag=dag,
    op_args=[con, service, 2020, sheets[2020]]
    )
    
task4 = PythonOperator(
    task_id='2021',
    python_callable=pull_from_sheet,
    dag=dag,
    op_args=[con, service, 2021, sheets[2021]]
    )

task5 = PythonOperator(
    task_id='2022',
    python_callable=pull_from_sheet,
    dag=dag,
    op_args=[con, service, 2022, sheets[2022]]
    )