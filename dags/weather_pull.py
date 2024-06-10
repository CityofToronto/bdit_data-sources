"""
Pipeline to pull weather prediction data from Envrionment Canada and upsert into weather.prediction_daily table.
Also scrapes historical weather for city and airport locations. Note the historical pull is run at 2:30AM to be
used in volume data check slack notifications, but the prediction is run at 1030am which is approximately when
the next day evenings forecast becomes available. 
A Slack notification is raised when the airflow process fails.
"""
import os
import sys
from pendulum import datetime, duration
from datetime import timedelta, time

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.sensors.time_sensor import TimeSensor

# DAG Information
DAG_NAME = 'weather_pull'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ["Unknown"])

#import python scripts
try:
    repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    sys.path.insert(0, repo_path)
    from weather.prediction_import import prediction_upsert
    from weather.historical_scrape import historical_upsert
    from dags.dag_functions import task_fail_slack_alert
except:
    raise ImportError("script import failed")

#DAG
 
default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past':False,
    'start_date': datetime(2024, 6, 3, tz="America/Toronto"),
    'email_on_failure': False,
    'email_on_success': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args, 
    schedule='30 6 * * *', #Historical weather is available at 1000UTC which is 6AM EDT and 5AM EST: https://climate.weather.gc.ca/FAQ_e.html#Q17
    catchup=False,
    tags=['weather', 'data_pull'],
    doc_md=__doc__
)
def weather_pull_dag():

    no_backfill = LatestOnlyOperator(task_id="no_backfill")
    no_backfill.doc_md = "Pull predicted weather data - can ONLY pull 5 days ahead of run date - no backfill."

    wait_till_1030am = TimeSensor(
        task_id="wait_till_1030am",
        timeout=5*3600,
        mode="reschedule",
        poke_interval=3600,
        target_time=time(hour = 10, minute = 30),
    )
    wait_till_1030am.doc_md = """
    The forecast for next 5 days is pulled at 1030am which is approximately
    when the forecast for next day evening becomes available.
    """

    @task()
    def pull_prediction():
        prediction_upsert(cred=PostgresHook("weather_bot"))
    pull_prediction.doc_md = "Pull weather forcast for 5 days ahead of run date"

    @task(
        retries=1,
        retry_delay=duration(hours=9) #late arriving data arrives 9 hours later: https://climate.weather.gc.ca/FAQ_e.html#Q17
    )
    def pull_historical(station_id, ds=None):
        historical_upsert(
            cred=PostgresHook("weather_bot"),
            run_date=ds,
            station_id=station_id
        )
    pull_historical.doc_md = "Pull yesterday's historical data for a given station id."
   
    no_backfill >> wait_till_1030am >> pull_prediction()
    pull_historical.override(task_id = 'pull_historical_city')(station_id=31688)
    pull_historical.override(task_id = 'pull_historical_airport')(station_id=51459)

weather_pull_dag()