# The official new GCC puller DAG file
from gcc_layer_refresh import get_layer

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

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
vfh32_layers = {"city_ward": [0, 0, 'bqu', True],
                #"centreline": [0, 2, 'bqu', False],
                "intersection": [12, 42, 'bqu', False],
                "ibms_grid": [11, 25, 'bqu', True],
                "centreline_intersection_point": [0, 19, 'bqu', False]
}

gcc_layers_dag = DAG(
    'pull_gcc_layers',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily'
)

for layer in vfh32_layers:
    pull_layer = PythonOperator(
        task_id = 'Task_'+ str(layer),
        python_callable = get_layer,
        op_args = vfh32_layers[layer]
    )