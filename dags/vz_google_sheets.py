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
from functools import partial

dag_name = 'vz_google_sheets'

dag_owners = Variable.get('dag_owners', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

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

#to read the python script for pulling data from google sheet and putting it into tables in postgres
try:
    # absolute path to the repo
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    # path of the python scripts
    sys.path.insert(0, repo_path)
    from gis.school_safety_zones.schools import pull_from_sheet
    from dags.dag_functions import task_fail_slack_alert
except:
    raise ImportError("Cannot import functions to pull school safety zone list")

def custom_fail_slack_alert(context: dict) -> str:
    """Adds a custom failure message in case of partial failure.

    Checks if the failing task completely failed or it just *partially* failed
    to pull some rows based on the xcom variable received from the failed task.

    Args:
        context: The calling Airflow task's context

    Returns:
        str: A string containing a custom message to get attached to the 
            standard failure alert.
    """
    invalid_rows = context.get(
        "task_instance"
    ).xcom_pull(
        task_ids=context.get("task_instance").task_id,
        key="invalid_rows"
    )
    if invalid_rows:
        return "Found one or more invalid records. Please, email David Tang "
    else:
        return ""

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
    'on_failure_callback': partial(
        task_fail_slack_alert, extra_msg=custom_fail_slack_alert
    )
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