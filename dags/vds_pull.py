import os
import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

#CONNECT TO ITS_CENTRAL
itsc_bot = PostgresHook('itsc_postgres')

#CONNECT TO BIGDATA
vds_bot = PostgresHook('vds_bot')

try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0,os.path.join(repo_path,'volumes/vds/py'))
    import vds_functions
except:
    raise ImportError("Cannot import functions from volumes/vds/py/vds_functions.py.")

dag_name = 'vds_pull'

# Get slack member ids
#dag_owners = Variable.get('dag_owners', deserialize_json=True)
#names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    
names = ['gabe']

SLACK_CONN_ID = 'slack_data_pipeline'
def task_fail_slack_alert(context):
    slack_ids = Variable.get('slack_member_id', deserialize_json=True)
    list_names = []
    for name in names:
        list_names.append(slack_ids.get(name, '@Unknown Slack ID')) #find slack ids w/default = Unkown

    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    
    log_url = context.get('task_instance').log_url.replace(
        'localhost', context.get('task_instance').hostname + ":8080"
    )
    
    slack_msg = """
        :ring_buoy: Task Failed. 
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
            log_url=log_url,
            slack_name=' '.join(list_names)
    )
    
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        proxy='http://'+BaseHook.get_connection('slack').password+'@137.15.73.132:8080',
        )
    return failed_alert.execute(context=context)
    
default_args = {
    'owner': ','.join(names),
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=60),
    'on_failure_callback': task_fail_slack_alert,
    'catchup':False,
}

#start_date = '2023-06-28'

with DAG(dag_name,
         default_args=default_args,
         schedule_interval='0 4 * * *') as dag: #daily at 4am

    with TaskGroup(group_id='vdsdata_complete') as vdsdata_complete:

        #this task group deletes any existing data from RDS vds.raw_vdsdata and then pulls and inserts from ITSC
        with TaskGroup(group_id='pull_vdsdata') as vdsdata:
            #deletes data from vds.raw_vdsdata
            delete_raw_vdsdata_task = PostgresOperator(
                sql="DELETE FROM vds.raw_vdsdata WHERE datetime_15min >= '{{ds}} 00:00:00'::timestamp AND datetime_15min < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'",
                task_id='delete_vdsdata',
                dag=dag,
                postgres_conn_id='vds_bot',
                autocommit=True,
                retries=1
            )

            #get vdsdata from ITSC and insert into RDS `vds.raw_vdsdata`
            pull_raw_vdsdata_task = PythonOperator(
                task_id='pull_raw_vdsdata',
                python_callable=vds_functions.pull_raw_vdsdata,
                dag=dag,
                op_kwargs = {
                        'rds_conn': vds_bot,
                        'itsc_conn': itsc_bot,
                        'start_date': '{{ ds }}'
                        } 
            )

            delete_raw_vdsdata_task >> pull_raw_vdsdata_task

        #this task group deletes any existing data from RDS vds.volumes_15min and then inserts into the same table
        with TaskGroup(group_id='summarize_v15') as v15data:
            #deletes data from vds.volumes_15min
            delete_v15_task = PostgresOperator(
                sql="DELETE FROM vds.volumes_15min WHERE datetime_bin >= '{{ds}} 00:00:00'::timestamp AND datetime_bin < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'",
                task_id='delete_v15',
                dag=dag,
                postgres_conn_id='vds_bot',
                autocommit=True,
                retries=1
            )

            #inserts summarized data into RDS `vds.volumes_15min`
            summarize_data_task = PostgresOperator(
                sql="SELECT vds.aggregate_15min_vds_volumes('{{ds}} 00:00:00'::timestamp, '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY')",
                task_id='summarize_data',
                dag=dag,
                postgres_conn_id='vds_bot',
                autocommit=True,
                retries=1
            )

            delete_v15_task >> summarize_data_task

        vdsdata >> v15data

    #this task group deletes any existing data from RDS vds.raw_vdsvehicledata and then pulls and inserts from ITSC
    with TaskGroup(group_id='pull_vdsvehicledata') as vdsvehicledata:
        #deletes data from vds.volumes_15min
        delete_vdsvehicledata_task = PostgresOperator(
            sql="DELETE FROM vds.raw_vdsvehicledata WHERE dt >= '{{ds}} 00:00:00'::timestamp AND dt < '{{ds}} 00:00:00'::timestamp + INTERVAL '1 DAY'",
            task_id='delete_vdsvehicledata',
            dag=dag,
            postgres_conn_id='vds_bot',
            autocommit=True,
            retries=1
        )

        #get vdsvehicledata from ITSC and insert into RDS `vds.raw_vdsvehicledata`
        pull_raw_vdsvehicledata_task = PythonOperator(
            task_id='pull_raw_vdsvehicledata',
            python_callable=vds_functions.pull_raw_vdsvehicledata,
            dag=dag,
            op_kwargs = {
                    'rds_conn': vds_bot,
                    'itsc_conn': itsc_bot,
                    'start_date': '{{ ds }}'
                    } 
        )

        delete_vdsvehicledata_task >> pull_raw_vdsvehicledata_task

    with TaskGroup(group_id='update_inventories') as update_inventories:
        #get vdsconfig from ITSC and insert into RDS `vds.vdsconfig`
        pull_detector_inventory_task = PythonOperator(
            task_id='pull_and_insert_detector_inventory',
            python_callable=vds_functions.pull_detector_inventory,
            dag=dag,
            op_kwargs = {
                'rds_conn': vds_bot,
                'itsc_conn': itsc_bot,
                },
        )

        #get entitylocations from ITSC and insert into RDS `vds.entity_locations`
        pull_entity_locations_task = PythonOperator(
            task_id='pull_and_insert_entitylocations',
            python_callable=vds_functions.pull_entity_locations,
            dag=dag,
            op_kwargs = {
                'rds_conn': vds_bot,
                'itsc_conn': itsc_bot,
                },
        )

        pull_detector_inventory_task
        pull_entity_locations_task

    vdsdata_complete
    vdsvehicledata
    update_inventories