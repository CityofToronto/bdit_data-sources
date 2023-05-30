# The official new GCC puller DAG file
import sys
import os
from datetime import datetime
from psycopg2 import sql
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0,os.path.join(repo_path,'gis/gccview'))
from gcc_puller_functions import get_layer
# Credentials - to be passed through PythonOperator
# bigdata connection credentials
bigdata_cred = PostgresHook("gcc_bot_bigdata")
# On-prem server connection credentials
ptc_cred = PostgresHook("gcc_bot")

SLACK_CONN_ID = 'slack_data_pipeline'
# Slack IDs of data pipeline admins
dag_config = Variable.get('slack_member_id', deserialize_json=True)
list_names = dag_config['raphael'] + ' ' + dag_config['islam'] + ' ' + dag_config['natalie'] 

def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed. 
            *Hostname*: {hostname}
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            {slack_name} please check.
            """.format(
            hostname=context.get('task_instance').hostname,
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
            slack_name=list_names
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        proxy='http://'+BaseHook.get_connection('slack').password+'@137.15.73.132:8080')
    return failed_alert.execute(context=context)

DEFAULT_ARGS = {
 'owner': 'natalie',
 'depends_on_past': False,
 'start_date': datetime(2022, 11, 3),
 'email_on_failure': False, 
 'email': ['natalie.chan@toronto.ca'], 
 'retries': 0,
 'on_failure_callback': task_fail_slack_alert
}

#-------------------------------------------------------------------------------------------------------
bigdata_layers = {"city_ward": [0, 0, 'gis_core', True], # VFH Layers
              "centreline": [0, 2, 'gis_core', False], # VFH Layers
              "ibms_grid": [11, 25, 'gis_core', True], # VFH Layers
              "centreline_intersection_point": [0, 19, 'gis_core', False], # VFH Layers
              
              #"intersection": [12, 42, 'gis_core', False],
              "census_tract": [26, 7, 'gis_core', True],
              "neighbourhood_improvement_area": [26, 11, 'gis_core', True],
              "priority_neighbourhood_for_investment": [26, 13, 'gis_core', True],
              
              #"bikeway": [2, 2, 'gis', True], #replaced by cycling_infrastructure
              "cycling_infrastructure": [2, 49, 'gis', True], 
              "traffic_camera": [2, 3, 'gis', True],
              "permit_parking_area": [2, 11, 'gis', True],
              "prai_transit_shelter": [2, 35, 'gis', True],
              "traffic_bylaw_point": [2, 38, 'gis', True],
              "traffic_bylaw_line": [2, 39, 'gis', True],
              "loop_detector": [2, 46, 'gis', True],
              "electrical_vehicle_charging_station": [20, 1, 'gis', True],
              "day_care_centre": [22, 1, 'gis', True],
              "middle_childcare_centre": [22, 2, 'gis', True],
              "business_improvement_area": [23, 1, 'gis', True],
              "proposed_business_improvement_area": [23, 13, 'gis', True],
              "film_permit_all": [23, 9, 'gis', True],
              "film_permit_parking_all": [23, 10, 'gis', True],
              "hotel": [23, 12, 'gis', True],
              "convenience_store": [26, 1, 'gis', True],
              "supermarket": [26, 4, 'gis', True],
              "place_of_worship": [26, 5, 'gis', True],
              "ymca": [26, 6, 'gis', True],
              "aboriginal_organization": [26, 45, 'gis', True],
              "attraction": [26, 46, 'gis', True],
              "dropin": [26, 47, 'gis', True],
              "early_years_centre": [26, 48, 'gis', True],
              "family_resource_centre": [26, 49, 'gis', True],
              "food_bank": [26, 50, 'gis', True],
              "longterm_care": [26, 53, 'gis', True],
              "parenting_family_literacy": [26, 54, 'gis', True],
              "retirement_home": [26, 58, 'gis', True],
              "senior_housing": [26, 59, 'gis', True],
              "shelter": [26, 61, 'gis', True],
              "social_housing": [26, 62, 'gis', True],
              "private_road": [27, 13, 'gis', True],
              "school": [28, 17, 'gis', True],
              "library": [28, 28, 'gis', True]
             }


ptc_layers = {"city_ward": [0, 0, 'gis', True],
                  "centreline": [0, 2, 'gis', False],
                  "intersection": [12, 42, 'gis', False],
                  "centreline_intersection_point": [0, 19, 'gis', False],
                  "ibms_grid": [11, 25, 'gis', True],
                  "ibms_district": [11, 23, 'gis', True]
                 }

# the DAG runs at 7 am on the first day of March, June, September, and December
with DAG(
    'pull_gcc_layers',
    catchup=False,
    default_args=DEFAULT_ARGS,
    schedule_interval='0 7 1 */3 *' #'@quarterly'
) as gcc_layers_dag:
    deployment = os.environ.get("DEPLOYMENT", "PROD")

    if deployment == "DEV":
        for layer in bigdata_layers:
            pull_bigdata_layer = PythonOperator(
                task_id = 'bigdata_task_'+ str(layer),
                python_callable = get_layer,
                op_args = bigdata_layers[layer] + [bigdata_cred]
            )

    for layer in ptc_layers:
        pull_ptc_layer = PythonOperator(
            task_id = 'VFH_task_'+ str(layer),
            python_callable = get_layer,
            op_args = ptc_layers[layer] + [ptc_cred]
        )

        if layer in ['centreline', 'intersection']:
            sql_refresh_mat_view = sql.SQL("SELECT {function_name}()").format(
                function_name=sql.Identifier('gis', f'refresh_mat_view_{layer}_version_date')
            )
            refresh_mat_view = PythonOperator(
                                    python_callable=lambda:ptc_cred.get_conn().cursor().execute(sql_refresh_mat_view),
                                    task_id=f'refresh_{layer}_version_date',
                                    retries = 0)
            
            pull_ptc_layer >> refresh_mat_view