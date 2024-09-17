"""
Pipeline to pull here data every week and put them into the here.ta table using Bash Operator.
Slack notifications is raised when the airflow process fails.
"""
import sys
import os
import pendulum

from datetime import timedelta
from airflow.hooks.base_hook import BaseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.decorators import dag, task, task_group
from airflow.macros import ds_add, ds_format

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
    from here.traffic.here_api import query_dates, get_access_token, get_download_url, HereAPIException
except:
    raise ImportError("Cannot import slack alert functions")

doc_md = """

### The Daily HERE pulling DAG [probe]

This DAG runs daily to pull here data from traffic analytics' API to here.ta in the bigdata database using Taskflow.
Slack notifications is raised when the airflow process fails. 
This pulles probe data in oppose to path in the pull_here_path DAG.

"""

dag_name = 'pull_here'
dag_owners = Variable.get('dag_owners', deserialize_json=True)
names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2020, 1, 5, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 3, #Retry 3 times
                'retry_delay': timedelta(minutes=60), #Retry after 60 mins
                'retry_exponential_backoff': True, #Allow for progressive longer waits between retries
                'on_failure_callback': task_fail_slack_alert,
                'env':{'LC_ALL':'C.UTF-8', #Necessary for Click
                       'LANG':'C.UTF-8'}
                }

@dag(dag_id = dag_name,
     default_args=default_args,
     schedule_interval='0 17 * * * ' ,
     catchup=False,
     doc_md = doc_md,
     tags=["HERE"]
     )

def pull_here():
    
    @task(retries=0) 
    def get_download_link(ds=None):
        api_conn = BaseHook.get_connection('here_api_key')
        pull_date = ds_format(ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d")
        i = 0
        while i < 3:
            try:
                access_token = get_access_token(api_conn.password, api_conn.extra_dejson['client_secret'], api_conn.extra_dejson['token_url'])
                if i == 0: #only generate request_id once, but new access_token on each attempt
                    request_id = query_dates(access_token, pull_date, pull_date, api_conn.host, api_conn.login, api_conn.extra_dejson['user_email'])
                download_url = get_download_url(request_id, api_conn.extra_dejson['status_base_url'], access_token, api_conn.login)
                return download_url
            except HereAPIException:
                i+=1
    
    download_url = get_download_link()

    @task.bash(env={"DOWNLOAD_URL": download_url,
                    "HOST":  BaseHook.get_connection("here_bot").host,
                    "USER" :  BaseHook.get_connection("here_bot").login,
                    "PGPASSWORD": BaseHook.get_connection("here_bot").password})
    def load_data_run()->str:
        return '''curl $DOWNLOAD_URL | gunzip | psql -h $HOST -U $USER -d bigdata -c "\COPY here.ta_view FROM STDIN WITH (FORMAT csv, HEADER TRUE);" '''
    
    # Create a task group for triggering the DAGs
    @task_group(group_id='trigger_dags_tasks')
    def trigger_dags(**kwargs):
        # Define TriggerDagRunOperator for each dag to trigger
        trigger_operators = []
        DAGS_TO_TRIGGER = Variable.get('here_dag_triggers', deserialize_json=True)
        for dag_id in DAGS_TO_TRIGGER:
            trigger_operator = TriggerDagRunOperator(
                task_id=f'trigger_{dag_id}',
                trigger_dag_id=dag_id,
                logical_date = '{{ ds }}',
                reset_dag_run = True # Clear existing dag if already exists (for backfilling), old runs will not be in the logs
            )
            trigger_operators.append(trigger_operator)

    load_data_run() >> trigger_dags()

pull_here()
