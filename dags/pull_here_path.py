import sys
import os
import pendulum
from datetime import timedelta

from airflow.decorators import task, dag
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable 
from airflow.macros import ds_add, ds_format

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from dags.dag_functions import task_fail_slack_alert
    from here.traffic.here_api import get_access_token, get_download_url
    from here.traffic.here_api_path import query_dates, HereAPIException
except:
    raise ImportError("Cannot import slack alert functions")

doc_md = """

### The Daily HERE pulling DAG

This DAG runs daily to pull here data from traffic analytics' API to here.ta in the bigdata database using Taskflow.
Slack notifications is raised when the airflow process fails.

"""
dag_name = 'pull_here_path'

dag_owners = Variable.get('dag_owners', deserialize_json=True)
names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

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
     schedule_interval='30 10 * * *' ,
     catchup=False,
     doc_md = doc_md,
     tags=["HERE"]
     )

def pull_here_path():

    @task(retries=0) 
    def get_download_link(ds=None):
        api_conn = BaseHook.get_connection('here_api_key')
        pull_date = ds_format(ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d")
        access_token = get_access_token(api_conn)
        request_id = query_dates(access_token, pull_date, pull_date, api_conn.host, api_conn.login, api_conn.extra_dejson['user_email'])
        download_url = get_download_url(request_id, api_conn.extra_dejson['status_base_url'], access_token, api_conn.login, api_conn)
        return download_url
    
    download_url = get_download_link()

    @task.bash(env={"DOWNLOAD_URL": download_url,
                    "HOST":  BaseHook.get_connection("here_bot").host,
                    "USER" :  BaseHook.get_connection("here_bot").login,
                    "PGPASSWORD": BaseHook.get_connection("here_bot").password})
    def load_data()->str:
        return '''curl $DOWNLOAD_URL | gunzip | psql -h $HOST -U $USER -d bigdata -c "\COPY here.ta_path_view FROM STDIN WITH (FORMAT csv, HEADER TRUE);" '''

    load_data()

pull_here_path()