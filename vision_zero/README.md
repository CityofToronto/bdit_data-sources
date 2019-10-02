# Vision Zero Google Sheets API 
This repository contains scripts to read Vision Zero google spreadsheets and put them into two postgres tables using Google Sheets API. This process is then automated using Airflow for it to run daily.

**Note:** 
Introduction to Google Sheets API can be found at [Intro](https://developers.google.com/sheets/api/guides/concepts)
A guide on how to get started can be found at [Quickstart](https://developers.google.com/sheets/api/quickstart/python)

## 1. Data source
The Google Sheets read are 2018 and 2019 School Safety Zones which are maintained by Mateen. The data are important as a dashboard indicator on VZ Dashboard. The Sheets are named `2018 School Safety Zone` and `2019 School Safety Zone`. Link to the Sheets can be found below:
- **2018:** [2018 School Safety Zone](https://docs.google.com/spreadsheets/d/16ZmWa6ZoIrJ9JW_aMveQsBM5vuGWq7zH0Vw_rvmSC7A/edit#gid=1848006623)
- **2019:** [2019 School Safety Zone](https://docs.google.com/spreadsheets/d/19JupdNNJSnHpO0YM5sHJWoEvKumyfhqaw-Glh61i2WQ/edit#gid=923156418)

## 2. Get started
In order to get started, a few things have to be done first.

### 2.1 Pip install
Run the following command to install the Google Client Library using pip:
`pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib`

### 2.2 Prepare credentials file
A credential file (named `key.json` in the script) is required to connect to the Google Sheets to pull data. The google account used to read the Sheets is `bdittoronto@gmail.com`. First, Google Sheets API have to be enabled on the google account. Then, a service account is created so that we are not prompted to sign in every single time we run the script. Instructions on how to do that can be found at [Creating a service account](https://github.com/googleapis/google-api-python-client/blob/master/docs/oauth-server.md#creating-a-service-account). Go to the `Service accounts` page from there, select the 'Quickstart' project and click on the 'Search for APIs and Services' bar to generate credentials. Copy the credentials and paste it on a `key.json` file located in the same directory as the script. The `key.json` file should look something like this:
`
{
    "type": "service_account",
    "project_id": "quickstart-1568664221624",
    "private_key_id": 
    "private_key":
    "client_email":
    "client_id": 
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": 
  }`

**Note:** A file `token.pickle` will be created if not found from the same directory. If the `SCOPES` is modified from readonly, the existing file `token.pickle` has to be deleted first.

### 2.3 Prepare connection file
A configuration file named `db.cfg` is required to connect to pgAdmin to push data aka to create postgres table. The `db.cfg` file should look like this:
`
[DBSETTINGS]
host=10.160.12.47
database=bigdata
user=database username
password=database password
`

## 3. Table generated
The script reads information from columns A, B, E, F, Y, Z, AA, AB which are 
SCHOOL NAME|ADDRESS|FLASHING BEACON W/O|WYSS W/O|School Coordinate (X,Y)|Final Sign Installation Date|FB Locations (X,Y)|WYS Locations (X,Y)|

from the Google Sheets and put them into postgres tables with the following fields (all in data type text):
school_name|address|work_order_fb|work_order_wyss|locations_zone|final_sign_installation|locations_fb|locations_wyss|
-----------|-------|-------------|---------------|--------------|-----------------------|------------|--------------|
AGINCOURT JUNIOR PUBLIC SCHOOL|29 Lockie Ave|9239020|9239021|43.788456, -79.281118|January 9, 2019|43.786566, -79.279023|43.787530, -79.279456|

**Notes:** The script reads up to line 180 on the spreadsheet in order to anticipate extra schools which might be added into the sheet in the future. The script works in a way that rows with empty cells at the beginning or end of the row or just an entire row of empty cells are not included in the postgres table.

## 4. Airflow
The Airflow is set up to run daily. A bot has to first be set up on pgAdmin to connect to Airflow. Connect to `/etc/airflow` on EC2 to create a dag file which contains the script for Airflow. More information on that can be found on [Credential management](https://github.com/CityofToronto/bdit_team_wiki/wiki/Automating-Stuff#credential-management). The DAG used for the vz google sheets API is as shown below:
`
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import sys

from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from googleapiclient.discovery import build

#to read the python script for pulling data from google sheet and putting it into tables in postgres
try:
  sys.path.append('/home/jchew/bdit_data-sources/vision_zero/')
  from schools import pull_from_sheet
except:
  raise ImportError("Cannot import functions to pull school safety zone list")

#to get credentials to access google sheets
vz_api_hook = GoogleCloudBaseHook('vz_api_google')
cred = vz_api_hook._get_credentials()
service = build('sheets', 'v4', credentials=cred, cache_discovery=False)

#to connect to pgadmin bot
vz_api_bot = PostgresHook("vz_api_bot")
con = vz_api_bot.get_conn()

DEFAULT_ARGS = {
    'owner': 'jchew',
    'depends_on_past' : False,
    'email': ['Joven.Chew@toronto.ca'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime(2019, 9, 30),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('vz_google_sheets', default_args=DEFAULT_ARGS, schedule_interval='@daily')

task1 = PythonOperator(
    task_id='2018',
    python_callable=pull_from_sheet,
    dag=dag,
    op_args=[con, service, 2018]
    )

task2 = PythonOperator(
    task_id='2019',
    python_callable=pull_from_sheet,
    dag=dag,
    op_args=[con, service, 2019]
    )

#run task1 and then task2 (task2 is dependent on task1 though)
task1 >> task2
`

**Note:** An empty __init__.py file then has to be created to run Airflow. 
