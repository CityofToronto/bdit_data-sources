from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('data_processing', default_args=default_args, schedule_interval='@daily')

def pull_raw_data():
    # Pull raw data from Postgres database
    pg_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    raw_data = pg_hook.get_records("SELECT * FROM your_raw_data_table")
    
    # Transform raw data
    transformed_data = your_transform_function(raw_data)
    
    # Get the current date
    current_date = datetime.now().date().isoformat()
    
    # Drop records for the current date
    drop_query = f"DELETE FROM rescu.raw_20sec WHERE date = '{current_date}'"
    pg_hook.run(drop_query)
    
    # Insert cleaned data into the database
    insert_query = f"INSERT INTO rescu.raw_20sec VALUES {transformed_data}"
    pg_hook.run(insert_query)

pull_raw_data_task = PythonOperator(
    task_id='pull_raw_data',
    python_callable=pull_raw_data,
    dag=dag
)

def summarize_data():
    # Perform summarization logic on raw_20sec table
    pg_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    summarize_query = '''
        INSERT INTO volumes_15min (summary_column1, summary_column2, summary_column3)
        SELECT SUM(column1), AVG(column2), COUNT(*)
        FROM rescu.raw_20sec
        WHERE date = current_date - interval '1 day'
        GROUP BY summary_column1, summary_column2
    '''
    pg_hook.run(summarize_query)

summarize_data_task = PythonOperator(
    task_id='summarize_data',
    python_callable=summarize_data,
    dag=dag
)

pull_raw_data_task >> summarize_data_task
