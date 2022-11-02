# The official new GCC puller DAG file
from gcc_layer_refresh import get_layer

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

# Credentials - to be passed through PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
# EC2 con
ec2_cred = PostgresHook("")
ec2_con = ec2_cred.get_conn()
# Morbius con
morbius_cred = PostgresHook("")
morbius_con = morbius_cred.get_conn()


SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url,
        )
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        proxy='http://137.15.73.132:8080')
    return failed_alert.execute(context=context)

DEFAULT_ARGS = {
 'owner': 'natalie',
 'depends_on_past': False,
 'start_date': datetime(2022, 10, 5),
 'email_on_failure': False, 
 'email': ['natalie.chan@toronto.ca'], 
 'retries': 0,
 'on_failure_callback': task_fail_slack_alert
}

#-------------------------------------------------------------------------------------------------------
ec2_layers = {"city_ward": [0, 0, 'gis_core', True], # VFH Layers
              "centreline": [0, 2, 'gis_core', False], # VFH Layers
              "ibms_grid": [11, 25, 'gis_core', True], # VFH Layers
              "centreline_intersection_point": [0, 19, 'gis_core', False], # VFH Layers
              
              #"intersection": [12, 42, 'gis_core', False],
              "census_tract": [26, 7, 'gis_core', True],
              "neighbourhood_improvement_area": [26, 11, 'gis_core', True],
              "priority_neighbourhood_for_investment": [26, 13, 'gis_core', True],
              
              "bikeway": [2, 2, 'gis', True],
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


morbius_layers = {"city_ward": [0, 0, 'gis', True],
                  "centreline": [0, 2, 'gis', False],
                  "intersection": [12, 42, 'gis', False],
                  "centreline_intersection_point": [0, 19, 'gis', False],
                  "ibms_grid": [11, 25, 'gis', True],
                  "ibms_district": [11, 23, 'gis', True]
                 }


gcc_layers_dag = DAG(
    'pull_gcc_layers',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily'
)

for layer in ec2_layers:
    pull_ec2_layer = PythonOperator(
        task_id = 'EC2_Task_'+ str(layer),
        python_callable = get_layer,
        op_args = ec2_layers[layer] + [ec2_con]
    )

for layer in morbius_layers:
    pull_morbius_layer = PythonOperator(
        task_id = 'Morbius_Task_'+ str(layer),
        python_callable = get_layer,
        op_args = morbius_layers[layer] + [morbius_con]
    )