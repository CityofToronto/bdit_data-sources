"""### open_data_checks DAG

- open data uids are stored in Airflow variable: `open_data_ids`
- `check_freshness` checks if datasets are outdated versus their stated refresh times
- `usage_stats` reports usage stats from the previous month based on stats released to Open Data 
"""

import os
import sys
import pendulum
from datetime import datetime, timedelta
from psycopg2 import sql
import pandas as pd

from airflow.sdk import dag, task

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, send_slack_msg
from bdit_dag_utils.utils.custom_operators import SQLCheckOperatorWithReturnValue
from dags.dag_owners import owners
from open_data.od_checks import (
    get_usage_stats, get_file_clicks, get_api_usage, get_resources, get_connection
)

DAG_NAME = 'open_data_checks'
DAG_OWNERS = owners.get(DAG_NAME, ['Unknown'])

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True, #Allow for progressive longer waits between retries
    #'on_failure_callback': task_fail_slack_alert,
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    max_active_runs=1,
    doc_md=__doc__,
    template_searchpath=os.path.join(repo_path,'open_data/sql'),
    tags=['bdit_data-sources', 'open_data', 'data_check'],
    schedule='0 0 5 * *', #fifth day of every month
    catchup=False,
)

def od_check_dag():
    
    @task
    def usage_stats():
        conn=get_connection()
        get_usage_stats(conn)
        
        
    @task
    def file_clicks():
        conn=get_connection()
        get_file_clicks(conn)
        
        
    @task
    def api_usage():
        conn=get_connection()
        get_api_usage(conn)
        
    @task
    def resources():
        conn=get_connection()
        get_resources(conn)
        
    check_outdated = SQLCheckOperatorWithReturnValue(
        task_id="check_outdated",
        sql="select-outdated.sql",
        conn_id="ref_bot",
        retries=0,
    )
   
    @task
    def check_usage_stats(**context):
        
        last_month = pendulum.now().subtract(months=1).set(day=1)
        fpath = os.path.join(repo_path, 'open_data/sql/select-monthly_stats.sql')
        with open(fpath, 'r', encoding="utf-8") as file:
            query = sql.SQL(file.read()).format(
                mnth = sql.Literal(last_month.to_date_string())
            )
        
        conn=get_connection()
        with conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
            df = pd.DataFrame(results, columns=cols)
        
        dt_md = df.to_markdown(index = False, tablefmt="slack")
        
        send_slack_msg(
            context=context,
            msg=f":open_data_to: page view analytics from `{last_month.format('MMMM Y')}`:```{dt_md}```"
        )

    usage_stats() >> file_clicks() >> api_usage() >> resources() >> (
        check_outdated,
        check_usage_stats()
    )

od_check_dag()