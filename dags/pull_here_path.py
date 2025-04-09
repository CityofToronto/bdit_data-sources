import sys
import os
import pendulum
from datetime import timedelta

from airflow.sdk import task, dag
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.execution_time.macros import ds_add, ds_format

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_owners import owners
    from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert
    from here.traffic.here_api import get_access_token, get_download_url
    from here.traffic.here_api_path import query_dates
except:
    raise ImportError("Cannot import slack alert functions")

doc_md = """

### The Daily HERE pulling DAG

This DAG runs daily to pull here data from traffic analytics' API to here.ta in the bigdata database using Taskflow.
Slack notifications is raised when the airflow process fails.

"""
dag_name = 'pull_here_path'

names = owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2023, 9, 26, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0, #Retry 3 times
                'retry_delay': timedelta(minutes=60), #Retry after 60 mins
                'retry_exponential_backoff': True, #Allow for progressive longer waits between retries
                'on_failure_callback': task_fail_slack_alert,
                'env':{'LC_ALL':'C.UTF-8', #Necessary for Click
                       'LANG':'C.UTF-8'}
                }

@dag(dag_id = dag_name,
     default_args=default_args,
     schedule='0 17 * * * ',
     catchup=False,
     doc_md = doc_md,
     tags=["HERE", "data_pull"]
     )

def pull_here_path():

    @task
    def send_request():
        api_conn = BaseHook.get_connection('here_api_key')
        access_token = get_access_token(api_conn)
        return access_token

    @task
    def get_request_id(access_token: str, ds=None):
        api_conn = BaseHook.get_connection('here_api_key')
        pull_date = ds_format(ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d")
        request_id = query_dates(access_token, pull_date, pull_date, api_conn.host, api_conn.login, api_conn.extra_dejson['user_email'])
        return request_id
    
    @task(retries=0) 
    def get_download_link(request_id: str, access_token: str):
        api_conn = BaseHook.get_connection('here_api_key')
        download_url = get_download_url(request_id, api_conn.extra_dejson['status_base_url'], access_token, api_conn.login, api_conn)
        return download_url
    
    access_token = send_request()
    request_id =  get_request_id(access_token)
    download_url = get_download_link(request_id, access_token)
    
    @task.bash(env = {
        'HOST': '{{ conn.here_bot.host }}',
        'LOGIN': '{{ conn.here_bot.login }}',
        'PGPASSWORD': '{{ conn.here_bot.password }}',
        'DOWNLOAD_URL': download_url
    })
    def load_data()->str:
        return '''curl $DOWNLOAD_URL | gunzip | psql -h $HOST -U $LOGIN -d bigdata -c "\\COPY here.ta_path_view FROM STDIN WITH (FORMAT csv, HEADER TRUE);" '''

    # Create a task group for triggering the DAGs
    @task_group
    def trigger_dags_tasks():
        # Define TriggerDagRunOperator for each DAG to trigger
        trigger_operators = []
        DAGS_TO_TRIGGER = Variable.get('here_path_dag_triggers', deserialize_json=True)
        for dag_id in DAGS_TO_TRIGGER:
            trigger_operator = TriggerDagRunOperator(
                task_id=f'trigger_{dag_id}',
                trigger_dag_id=dag_id,
                logical_date='{{macros.ds_add(ds, 1)}}',
                reset_dag_run=True # Clear existing dag if already exists (for backfilling), old runs will not be in the logs
            )
            trigger_operators.append(trigger_operator)

    load_data() >> trigger_dags_tasks()

pull_here_path()
