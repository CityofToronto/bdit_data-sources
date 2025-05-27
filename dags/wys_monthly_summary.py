"""
Refresh WYS Materialized Views and run monthly aggregation function for Open Data.
A Slack notification is raised when the airflow process fails.
"""
import sys
import os
import pendulum
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert, get_readme_docmd

dag_name = 'wys_monthly_summary'
dag_owners = Variable.get('dag_owners', deserialize_json=True)
names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

README_PATH = os.path.join(repo_path, 'wys/api/readme.md')
DOC_MD = get_readme_docmd(README_PATH, dag_name)

default_args = {'owner': ','.join(names),
                'depends_on_past':False,
                'start_date': pendulum.datetime(2020, 4, 30, tz="America/Toronto"),
                'email_on_failure': False,
                 'email_on_success': False,
                 'retries': 0,
                 'retry_delay': timedelta(minutes=5),
                 'on_failure_callback': task_fail_slack_alert
                }

def last_month(ds):
    dt = datetime.strptime(ds, "%Y-%m-%d")
    # Don't need to add `months=1` because that seems to be taken care of with 
    # the monthly scheduling 
    return (dt - relativedelta(day=1)).strftime("%Y-%m-%d")

with DAG(dag_id = dag_name,
         default_args=default_args,
         max_active_runs=1,
         tags=["wys", "aggregation", "monthly"],
         doc_md = DOC_MD,
         user_defined_macros={
            'last_month' : last_month
          },
         schedule='0 3 2 * *') as monthly_summary:
    wys_view_stat_signs = SQLExecuteQueryOperator(
                            #sql in bdit_data-sources/wys/api/sql/mat-view-stationary-signs.sql
                            sql='SELECT wys.refresh_mat_view_stationary_signs()',
                            task_id='wys_view_stat_signs',
                            conn_id='wys_bot',
                            autocommit=True,
                            retries = 0)
    wys_view_mobile_api_id = SQLExecuteQueryOperator(
                            #sql in bdit_data-sources/wys/api/sql/function-refresh_mat_view_mobile_api_id.sql
                            #sql in bdit_data-sources/wys/api/sql/create-view-mobile_api_id.sql
                            sql='SELECT wys.refresh_mat_view_mobile_api_id()', 
                            task_id='wys_view_mobile_api_id',
                            conn_id='wys_bot',
                            autocommit=True,
                            retries = 0)
    od_wys_view = SQLExecuteQueryOperator(
                            #sql in bdit_data-sources/wys/api/sql/open_data/mat-view-stationary-locations.sql
                            sql='SELECT wys.refresh_od_mat_view()',
                            task_id='od_wys_view',
                            conn_id='wys_bot',
                            autocommit=True,
                            retries = 0)
    wys_mobile_summary = SQLExecuteQueryOperator(
                            #sql in bdit_data-sources/wys/api/sql/function-mobile-summary.sql
                            sql="SELECT wys.mobile_summary_for_month('{{ last_month(ds) }}')",
                            task_id='wys_mobile_summary',
                            conn_id='wys_bot',
                            autocommit=True,
                            retries = 0)
    wys_stat_summary = SQLExecuteQueryOperator(
                            #sql in bdit_data-sources/wys/api/sql/function-stationary-sign-summary.sql
                            sql="SELECT wys.stationary_summary_for_month('{{ last_month(ds) }}')", 
                            task_id='wys_stat_summary',
                            conn_id='wys_bot',
                            autocommit=True,
                            retries = 0)

    # Stationary signs
    wys_view_stat_signs >> [wys_stat_summary, od_wys_view]
    # Mobile signs
    wys_view_mobile_api_id >> wys_mobile_summary