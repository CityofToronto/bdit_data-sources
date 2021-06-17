"""
WORK IN PROGRESS DO NOT USE

"""
Pipeline to backfill here data and put them into the here.ta table using Bash Operator.
Slack notifications is raised when the airflow process fails.
"""

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.postgres_hook import PostgresHook

# Slack alert
SLACK_CONN_ID = 'slack'
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    task_msg = '<@UF4RQFQ11>.... :meow_verysad: {task_id} in backfilling here DAG very sadly failed.'.format(task_id=context.get('task_instance').task_id)   
    slack_msg = task_msg + """(<{log_url}|log>)""".format(
            log_url=context.get('task_instance').log_url,)
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        )
    return failed_alert.execute(context=context)

here_postgres = PostgresHook("here_bot")
rds_con = here_postgres.get_uri()
s3_conn_id = 'conn_S3'

default_args = {'owner':'natalie',
                'depends_on_past':True,
                'start_date': datetime(2017, 12, 1),
                'email': ['raphael.dumas@toronto.ca'],
                'email_on_failure': False,
                'email_on_success': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'on_failure_callback': task_fail_slack_alert,
                'env':{'here_bot':rds_con,
                        'LC_ALL':'C.UTF-8', #Necessary for Click
                        'LANG':'C.UTF-8'}
                }

# monthly
dag = DAG('backfill_here',default_args=default_args, schedule_interval='monthly')

# functions
def backup_to_s3_month(con, month, s3):
    '''Backup a month of ta data to specified s3
    '''
    with tempfile.TemporaryDirectory() as tempdir:
        #backup month data to compressed file
        copy_query = sql.SQL("COPY here.{} TO STDOUT WITH (FORMAT 'csv', HEADER TRUE)")
        #compress results of the copy command to a file in the compressed directory
        os.chdir(tempdir)
        data_file_path = 'here_{}.csv.gz'.format(month)
        with gzip.GzipFile(filename=data_file_path, mode='w') as data_file:
            with con.cursor() as cur:
                cur.copy_expert(copy_query.format(sql.Identifier('ta_'+month)), data_file)      
        #copy file to s3 bucket
        subprocess.check_call(['aws','s3','cp', data_file_path, s3])

        
        
#Tasks        
backup_data = PythonOperator(
                            task_id = 'task_backup_to_S3',
                            python_callable = backup_to_s3_month,
                            dag=dag,
                            op_kwargs={
                              'con': con,
                              # execution date is by default a day before if the process runs daily
                              'month': '{{ ds }}', 
                              's3' : s3_conn_id}
                            )

drop_and_create_table = PostgresOperator(sql='''here.create_partition_table('{{ ds }}') ''',
                            task_id='task_drop_create_table',
                            postgres_conn_id='natalie',
                            autocommit=True,
                            retries = 0,
                            dag=dag)

pull_data = BashOperator(
        task_id = 'pull_here',
        bash_command = '/etc/airflow/data_scripts/.venv/bin/python3 /etc/airflow/data_scripts/here/traffic/reimport_monthly.py -d /etc/airflow/data_scripts/here/traffic/config.cfg -m {{ ds }}', 
        retries = 0,
        dag=dag,
        )

# Backup here.ta_yyyymm table to S3 first, then drop and create a new here.ta_yyyymm with check constraint, then finally pull new data in
backup_data >> drop_and_create_table >>  pull_data