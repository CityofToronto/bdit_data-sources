import sys
import os
import pendulum
import datetime

from airflow.sdk import task, dag, Param, task_group, Variable
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.execution_time.macros import ds_add, ds_format
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

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
dag_name = 'pull_here_path_hm'

names = owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2023, 9, 26, tz="America/Toronto"),
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0, #Retry 3 times
                'retry_delay': datetime.timedelta(minutes=60), #Retry after 60 mins
                'retry_exponential_backoff': True, #Allow for progressive longer waits between retries
                'on_failure_callback': task_fail_slack_alert,
                'env':{'LC_ALL':'C.UTF-8', #Necessary for Click
                       'LANG':'C.UTF-8'}
                }

@dag(dag_id = dag_name,
     default_args=default_args,
     schedule='0 17 * * * ',
     catchup=False,
     max_active_runs=10,
     doc_md = doc_md,
     tags=["HERE", "data_pull"],
     #can also use params via cli. A max of 1 week is recommended.
     #airflow dags trigger --conf '{"start_date": "2024-02-01", "end_date": "2024-02-07"}' pull_here_path_hm
     params={
            "start_date": Param(
                default=None,
                type=["null", "string"],
                format="date",
                title="Start Date",
                description="Custom start date for use in backfilling.",
            ),
            "end_date": Param(
                default=None,
                type=["null", "string"],
                format="date",
                title="End Date",
                description="Custom end date for use in backfilling. Date is inclusive.",
        )}
     )

def pull_here_path():

    @task
    def send_request():
        api_conn = BaseHook.get_connection('here_api_key')
        access_token = get_access_token(api_conn)
        return access_token

    @task(multiple_outputs=True)
    def get_request_id(access_token: str, ds=None, **context):
        start_date = context["params"]["start_date"]
        end_date = context["params"]["end_date"]
        if start_date is None:
            start_date = ds_format(ds_add(ds, -1), "%Y-%m-%d", "%Y%m%d")
            end_date = start_date
        else:
            start_date = start_date.replace("-", "")
            end_date = end_date.replace("-", "")
        
        api_conn = BaseHook.get_connection('here_api_key')
        request_id = query_dates(access_token, start_date, end_date, api_conn.host, api_conn.login, api_conn.extra_dejson['user_email'])
        return {
            "request_id": request_id,
            "start_date": start_date,
            "end_date": end_date,
        }
    
    create_annual_partition = SQLExecuteQueryOperator(
        task_id='create_annual_partitions',
        sql="""SELECT here.create_weekly_partitions(
                yr := date_part('year', '{{ ti.xcom_pull(task_ids='get_request_id', key='start_date') }}'::date)::int,
                schem := 'here',
                tbl := 'ta_path_hm'
            )""",
        conn_id='here_bot',
    )

    clear_ta_path = SQLExecuteQueryOperator(
        sql="""DELETE FROM here.ta_path_hm
            WHERE tx >= '{{ ti.xcom_pull(task_ids='get_request_id', key='start_date') }}'::date
                AND tx < '{{ ti.xcom_pull(task_ids='get_request_id', key='end_date') }}'::date + interval '1 day';""",
        task_id='clear_ta_path',
        conn_id="here_bot",
        retries = 0
    )
    
    @task(retries=0)
    def get_download_link(request_id: str, access_token: str):
        api_conn = BaseHook.get_connection('here_api_key')
        download_url = get_download_url(request_id, api_conn.extra_dejson['status_base_url'], access_token, api_conn.login, api_conn)
        return download_url
    
    access_token = send_request()
    request_id =  get_request_id(access_token)
    download_url = get_download_link(request_id["request_id"], access_token)
    
    @task.bash(env = {
        'HOST': '{{ conn.here_bot.host }}',
        'LOGIN': '{{ conn.here_bot.login }}',
        'PGPASSWORD': '{{ conn.here_bot.password }}',
        'DOWNLOAD_URL': download_url
    },
    max_active_tis_per_dag=1)
    def load_data()->str:
        return '''
            curl --progress-bar --no-buffer -S $DOWNLOAD_URL \
                2> >(tr '\\r' '\\n' >&2) \
            | gunzip \
            | psql -h $HOST -U $LOGIN -d bigdata \
                -c "\\COPY here.ta_path_hm_view FROM STDIN WITH (FORMAT csv, HEADER TRUE);"
        '''

    # Create a task group for triggering the DAGs
    # need to figure out how to backfill multiple days
    @task_group
    def trigger_dags_tasks():
        # Define TriggerDagRunOperator for each DAG to trigger
        trigger_operators = []
        DAGS_TO_TRIGGER = Variable.get('here_path_dag_triggers', deserialize_json=True)
        for dag_id in DAGS_TO_TRIGGER:
            trigger_operator = TriggerDagRunOperator(
                task_id=f'trigger_{dag_id}',
                trigger_dag_id=dag_id,
                logical_date='{{macros.ds_add(ds, -1)}}',
                reset_dag_run=True # Clear existing dag if already exists (for backfilling), old runs will not be in the logs
            )
            trigger_operators.append(trigger_operator)

    create_annual_partition.set_upstream(request_id)
    create_annual_partition >> clear_ta_path
    clear_ta_path >> load_data() >> trigger_dags_tasks()
    
pull_here_path()
