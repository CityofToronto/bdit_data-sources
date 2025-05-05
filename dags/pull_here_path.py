import sys
import os
import pendulum
from datetime import timedelta

from airflow.decorators import task, dag, task_group
from airflow.hooks.base import BaseHook
from airflow.models import Variable 
from airflow.macros import ds_add, ds_format
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from here.traffic.here_api import get_access_token, get_download_url
    from here.traffic.here_api_path import query_dates
    from dags.dag_functions import task_fail_slack_alert, slack_alert_data_quality
    from dags.custom_operators import SQLCheckOperatorWithReturnValue
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
     schedule='0 17 * * * ',
     catchup=False,
     template_searchpath=os.path.join(repo_path,'here/traffic/sql'),
     doc_md = doc_md,
     tags=["HERE", "data_pull", "data_checks"]
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
    
    @task_group(
        tooltip="Tasks to check critical data quality measures which could warrant re-running the DAG."
    )
    def data_checks():
        insert_daily_summary = SQLExecuteQueryOperator(
            task_id='insert_daily_summary',
            sql="SELECT here.ta_path_daily_summary_insert('{{ (ds) }}'::date - 1)",
            conn_id='here_bot',
            autocommit=True
        )
        
        data_check_params = {
            "lookback": '30 days',
            "threshold": 0.7
        }
        check_row_count = SQLCheckOperatorWithReturnValue(
            on_failure_callback=slack_alert_data_quality,
            task_id="check_row_count",
            sql="ta_path_row_count_check.sql",
            conn_id="here_bot",
            retries=0,
            params=data_check_params | {"col_to_sum": "sum_sample_size"},
        )
        check_row_count.doc_md = '''
        Compare the row count today with the average row count from the lookback period.
        '''

        check_distinct_link_dirs = SQLCheckOperatorWithReturnValue(
            on_failure_callback=slack_alert_data_quality,
            task_id="check_distinct_link_dirs",
            sql="ta_path_row_count_check.sql",
            conn_id="here_bot",
            retries=0,
            params=data_check_params | {"col_to_sum": "num_link_dirs"},
        )
        check_distinct_link_dirs.doc_md = '''
        Compare the count of link_dirs appearing in today's pull vs the lookback period.
        '''
        
        insert_daily_summary >> [check_row_count, check_distinct_link_dirs]

    load_data() >> data_checks()

pull_here_path()
