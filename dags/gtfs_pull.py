"""### gtfs_pull DAG

- Pulls TTC GTFS from Toronto Open Data (id = ttc-routes-and-schedules)
- Checks daily to see if OD `last_refreshed` flag has been updated, if so uploads to bigdata
"""

import os
import io
import re
import sys
import zipfile
import logging
import requests
import psycopg2
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.taskinstance import TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DAG_NAME = 'gtfs_pull'
DAG_OWNERS = Variable.get('dag_owners', deserialize_json=True).get(DAG_NAME, ['Unknown'])

repo_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
sys.path.insert(0, repo_path)
from bdit_dag_utils.utils.dag_functions import task_fail_slack_alert

default_args = {
    'owner': ','.join(DAG_OWNERS),
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True, #Allow for progressive longer waits between retries
    'on_failure_callback': task_fail_slack_alert,
}

@dag(
    dag_id=DAG_NAME,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath=os.path.join(repo_path,'ttc/gtfs'),
    doc_md=__doc__,
    tags=['bdit_data-sources', 'open_data', 'data_check'],
    schedule='@daily',
    catchup=False,
)

def gtfs_pull():
    @task()
    def download_url(ti: TaskInstance | None = None):
        """Gets download_url and tries to insert `last_refreshed` attribute to feed_info table.
        
        If `last_refreshed` insert fails on duplicate, DAG is marked as skipped.
        Otherwise, proceed to download files and insert.
        """
        #get open data metadata
        url = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show"
        od_id = 'ttc-routes-and-schedules'
        params = { "id": od_id}
        package = requests.get(url, params = params).json()
        
        #get last_refreshed and url attributes
        try:
            result = package.get('result')
            download_url = result['resources'][0].get('url')
            last_refreshed = result.get('last_refreshed')
            ti.xcom_push(key="last_refreshed", value=last_refreshed)
        except KeyError as e:
            LOGGER.error("Problem retrieving Open Data portal info.")
            raise AirflowFailException(e)
        LOGGER.info("`%s` last_refreshed: %s", od_id, last_refreshed)
        
        #try inserting `last_refreshed` into feed_info table. Skips DAG on UniqueViolation
        query="INSERT INTO gtfs.feed_info(insert_date) SELECT %s RETURNING feed_id;"
        con = PostgresHook("gtfs_bot").get_conn()
        try:
            with con.cursor() as cur:
                cur.execute(query, (last_refreshed, ))
                feed_id = cur.fetchone()[0]
                con.commit()
        except psycopg2.errors.UniqueViolation:
            raise AirflowSkipException(f'This feed (last_refreshed = {last_refreshed}) has already been loaded into the database.')
            
        #feed_id is needed to update tables in `update_feed_id`
        ti.xcom_push(key="feed_id", value=feed_id)
        
        return download_url

    @task()
    def download_gtfs(download_url, ti: TaskInstance | None = None):
        """Downloads data and saves in `open_data/gtfs` folder inside Airflow home directory."""
        
        #download file
        try:
            gtfs_download=requests.get(download_url)
            gtfs_download.raise_for_status()
        except requests.exceptions.HTTPError as err_h:
            LOGGER.error("Invalid HTTP response: %s", err_h)
        except requests.exceptions.ConnectionError as err_c:
            LOGGER.error("Network problem: %s", err_c)
        except requests.exceptions.Timeout as err_t:
            LOGGER.error("Timeout: %s", err_t)
        except requests.exceptions.RequestException as err:
            LOGGER.error("Error: %s", err)
        else:
            if gtfs_download.status_code != 200:
                LOGGER.error("Query was not successful. Response: %s", gtfs_download)
    
        #create new folder
        last_refreshed = str(ti.xcom_pull(key='last_refreshed', task_ids='download_url'))
        last_refreshed_folder = re.sub(':|-', '', last_refreshed)
        last_refreshed_folder = last_refreshed_folder.replace(' ', '_')
        dir = "/data/airflow/open_data/gtfs/" + last_refreshed_folder
        try:
            os.makedirs(dir)
        except FileExistsError:
            raise AirflowFailException(f'Directory already exists: {dir}')
        
        #unzip download
        z = zipfile.ZipFile(io.BytesIO(gtfs_download.content))
        z.extractall(dir)
        return dir
    
    @task.bash(
        env = {
            'HOST': '{{ conn.gtfs_bot.host }}',
            'LOGIN': '{{ conn.gtfs_bot.login }}',
            'PGPASSWORD': '{{ conn.gtfs_bot.password }}'
        }
    )
    def upload_feed(dir):
        """Copies data into database. Fails on any errors."""
        return f"""
        set -e  # Exit on any error
        cd {dir}
        /usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c "\COPY gtfs.calendar(service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date) FROM 'calendar.txt' WITH (FORMAT 'csv', HEADER TRUE) ;"
        /usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c "\COPY gtfs.calendar_dates(service_id, date_, exception_type) FROM 'calendar_dates.txt' WITH (FORMAT 'csv', HEADER TRUE) ;"
        /usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c "\COPY gtfs.routes(route_id, agency_id, route_short_name, route_long_name, route_desc, route_type, route_url, route_color, route_text_color) FROM 'routes.txt' WITH (FORMAT 'csv', HEADER TRUE) ;"
        /usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c "\COPY gtfs.shapes(shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled) FROM 'shapes.txt' WITH (FORMAT 'csv', HEADER TRUE) ;"
        /usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c "\COPY gtfs.stop_times(trip_id, arrival_time, departure_time, stop_id, stop_sequence, stop_headsign, pickup_type, drop_off_type, shape_dist_traveled) FROM 'stop_times.txt' WITH (FORMAT 'csv', HEADER TRUE) ;"
        /usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c "\COPY gtfs.stops(stop_id, stop_code, stop_name, stop_desc, stop_lat, stop_lon, zone_id, stop_url, location_type, parent_station, stop_timezone, wheelchair_boarding) FROM 'stops.txt' WITH (FORMAT 'csv', HEADER TRUE) ;"
        /usr/bin/psql -h $HOST -U $LOGIN -d bigdata -c "\COPY gtfs.trips(route_id, service_id, trip_id, trip_headsign, trip_short_name, direction_id, block_id, shape_id, bikes_allowed, wheelchair_accessible) FROM 'trips.txt' WITH (FORMAT 'csv', HEADER TRUE) ;"
        rm -rf {dir}
        """

    #Update feed_id attribute across all the tables
    update_feed_id = SQLExecuteQueryOperator(
        task_id='update_feed_id',
        conn_id='gtfs_bot',
        sql="""UPDATE gtfs.calendar_imp SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;
        UPDATE gtfs.calendar_dates_imp SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;
        UPDATE gtfs.routes SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;
        UPDATE gtfs.shapes SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;
        UPDATE gtfs.shapes_geom SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;
        UPDATE gtfs.stop_times SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;
        UPDATE gtfs.stops SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;
        UPDATE gtfs.trips SET feed_id = {{ ti.xcom_pull(task_ids="download_url", key="feed_id") }} WHERE feed_id IS NULL;""",
        autocommit=True,
        retries = 0
    )
    
    download_url = download_url()
    output_dir = download_gtfs(download_url)
    upload_feed(output_dir) >> update_feed_id

gtfs_pull()