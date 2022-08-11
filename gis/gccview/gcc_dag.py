from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
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


default_args = {
 'owner': 'natalie',
 'depends_on_past': False,
 'start_date': datetime(2019, 10, 1),
 'email_on_failure': False, 
 'email': ['natalie.chan@toronto.ca'], 
 'retries': 0,
 'on_failure_callback': task_fail_slack_alert
}

dag = DAG('pull_gccview_layers', 
 schedule_interval='@monthly', 
 default_args=default_args 
 ) 

# define list of lobs we want to run 
layer_list = [[0,0],[2,2],[2,3],[2,11],[2,35],[2,37],[2,38],[2,39],[20,1],
             [22,1],[22,2],[23,1],[23,13],[23,9],[23,10],[23,12],[26,1],[26,4],[26,3],[26,6],
             [26,7],[26,11],[26,13],[26,16],[26,45],[26,46],[26,47],[26,48],[26,49],[26,50],
             [26,53],[26,54],[26,58],[26,59],[26,61],[26,62],[27,13],[28,17],[28,28]]
command = '''python /home/natalie/airflow/tasks/gcc_layer.py --mapserver_n={{params.mapserver_n}} --id={{params.id}}'''
# loop through the lob's we want to use to build up our dag
for i in layer_list:
    generate_tasks = BashOperator(dag = dag, 
                                 task_id='Task_'+ str(i[0])+'_'+str(i[1]), 
                                 bash_command=command,
                                 params={"mapserver_n": i[0], 'id':i[1]}, 
                                 )

""" New code """
# Enter the new set of layer types, wait for further instructions
layer_types = {
    
    
    
                }
# Create a task for each of the layers
pull_centreline = PythonOperator()
pull_ward = PythonOperator()