"""
Pipeline to pull weather prediction data from Envrionment Canada and upsert into weather.prediction_daily table.
Then, 
A Slack notification is raised when the airflow process fails.
"""
import os
import sys
import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.latest_only import LatestOnlyOperator

# DAG Information
dag_name = 'pull_weather'

#connection credentials
cred = PostgresHook("weather_bot")

#import python scripts
try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from weather.prediction_import import prediction_upsert
    from weather.historical_scrape import historical_upsert
    from dags.dag_functions import task_fail_slack_alert
except:
    raise ImportError("script import failed")


dag_owners = Variable.get('dag_owners', deserialize_json=True)

names = dag_owners.get(dag_name, ['Unknown']) #find dag owners w/default = Unknown    

#DAG
 
default_args = {
    'owner': 'Natalie',
    'depends_on_past':False,
    'start_date': pendulum.datetime(2022, 11, 8, tz="America/Toronto"),
    'end_date': pendulum.datetime(2024, 6, 3, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

dag = DAG(
    dag_id = dag_name, 
    default_args=default_args, 
    schedule='30 10 * * *',
    tags=['weather', 'data_pull'],
    catchup=False)

#=======================================#
# Pull predicted weather data - can ONLY pull 5 days ahead of run date - no backfill.
no_backfill = LatestOnlyOperator(task_id="no_backfill", dag=dag)

# TASKS

## Pull weather forcast for 5 days ahead of run date
PULL_PREDICTION = PythonOperator(
    task_id = 'pull_prediction',
    python_callable = prediction_upsert,
    dag=dag,
    op_args=[cred]
)

## Pull yesterday's historical data for Toronto city centre
PULL_HISTORICAL_CITY = PythonOperator(
    task_id = 'pull_historical_city',
    python_callable = historical_upsert,
    dag=dag,
    op_args=[cred, '{{ yesterday_ds }}', 31688]
)
## Pull yesterday's historical data for Toronto Peason Airport
PULL_HISTORICAL_AIRPORT = PythonOperator(
    task_id = 'pull_historical_airport',
    python_callable = historical_upsert,
    dag=dag,
    op_args=[cred, '{{ yesterday_ds }}', 51459]
)

no_backfill >> PULL_PREDICTION
PULL_HISTORICAL_CITY
PULL_HISTORICAL_AIRPORT
