# Vision Zero Google Sheets API 
This folder contains scripts to read Vision Zero google spreadsheets and put them into two postgres tables using Google Sheets API. This process is then automated using Airflow for it to run daily.

**Notes:** 
- Introduction to Google Sheets API can be found at [Intro](https://developers.google.com/sheets/api/guides/concepts).
- A guide on how to get started can be found at [Quickstart](https://developers.google.com/sheets/api/quickstart/python).

## 1. Data source
The School Safety Zone data are read in from separate Google Sheets for 2018, 2019, 2020 and 2021 which are maintained internally.

## 2. Get started
In order to get started, a few things have to be done first.

### 2.1 Pip install
Run the following command to install the Google Client Library using pip:
`pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib`

### 2.2 Prepare credentials file
A credential file (named `key.json` in the script) is required to connect to the Google Sheets to pull data. The google account used to read the Sheets is `bdittoronto@gmail.com`. First, Google Sheets API have to be enabled on the google account. Then, a service account is created so that we are not prompted to sign in every single time we run the script. Instructions on how to do that can be found at [Creating a service account](https://github.com/googleapis/google-api-python-client/blob/master/docs/oauth-server.md#creating-a-service-account). Go to the `Service accounts` page from there, select the `Quickstart` project and click on the `Search for APIs and Services` bar to generate credentials. Copy the credentials and paste it on a `key.json` file located in the same directory as the script. The `key.json` file should look something like this:

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

### 2.3 Prepare connection file
A configuration file named `db.cfg` is required to connect to pgAdmin to push data aka to create postgres table. The `db.cfg` file should look like this:

```
[DBSETTINGS]
host=xx.xxx.xx.xx
database=databasename
user=database username
password=database password
```

## 3. Adding a new year
Follow these steps to read in another spreadsheet for year `yyyy`.

### 3.1 Create empty table in database to store spreadsheet
Create an empty table `vz_safety_programs_staging.school_safety_zone_yyyy_raw`, where `yyyy` is the year to be stored, as a child of parent table `vz_safety_programs_staging.school_safety_zone_raw_parent`. Follow the format of the existing child tables (e.g. `vz_safety_programs_staging.school_safety_zone_2018_raw`) and declare the inheritance:

```
CREATE TABLE vz_safety_programs_staging.school_safety_zone_yyyy_raw (
   	like vz_safety_programs_staging.school_safety_zone_2018_raw 
	including all
) INHERITS (vz_safety_programs_staging.school_safety_zone_raw_parent);

```

### 3.2 Add id of google sheet to Airflow variable
In Airflow is already defined a dictionary containing key-value pairs to store the sheet `id` for each sheet. Go to Admin -> Variables in the Airflow GUI and edit `ssz_spreadsheet_ids` to add a new key `sszyyyy` for year `yyyy` and the `id` of the Google sheet for that year.



### 3.3 Edit script that reads in the spreadsheets
The `id` for each sheet is then called in `schools.py` by reading from the stored dictionary. Add a new call statement for the year to be added:

```
from airflow.models import Variable
dag_config = Variable.get('ssz_spreadsheet_ids', deserialize_json=True)
ssz2018 = dag_config['ssz2018']
sszyyyy = dag_config['sszyyyy']
```

Next, add to the `sheets` dictionary just below the variable calls the details about the sheet for the new year:

```
sheets = {
           2018: {'spreadsheet_id' : ssz2018,
                  'range_name' : 'Master List!A4:AC180',
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_2018_raw'},
           yyyy: {'spreadsheet_id' : sszyyyy,
                  'range_name' : 'Master Sheet!A3:AC180',
                  'schema_name': 'vz_safety_programs_staging',
                  'table_name' : 'school_safety_zone_yyyy_raw'}
         }
```

Finally, at the end of `schools.py`, add a new call to pull from the sheet for year `yyyy`:

```
pull_from_sheet(con, service, yyyy)
```

### 3.4 Edit the dag
Follow the general instructions in the `bdit_data-sources` [README](https://github.com/CityofToronto/bdit_data-sources/tree/master/dags).

Add a new task in `bdit_data-sources/dags/vz_google_sheets.py`:
e.g.:

```
task4 = PythonOperator(
    task_id='yyyy',
    python_callable=pull_from_sheet,
    dag=dag,
    op_args=[con, service, yyyy]
    )
```


## 4. Table generated
The script reads information from columns A, B, E, F, Y, Z, AA, AB which are as shown below

|SCHOOL NAME|ADDRESS|FLASHING BEACON W/O|WYSS W/O|School Coordinate (X,Y)|Final Sign Installation Date|FB Locations (X,Y)|WYS Locations (X,Y)|
|-----------|-------|-------------|---------------|--------------|-----------------------|------------|--------------|
|AGINCOURT JUNIOR PUBLIC SCHOOL|29 Lockie Ave|9239020|9239021|43.788456, -79.281118|January 9, 2019|43.786566, -79.279023|43.787530, -79.279456|

from the Google Sheets and put them into postgres tables with the following fields (all in data type text):

|school_name|address|work_order_fb|work_order_wyss|locations_zone|final_sign_installation|locations_fb|locations_wyss|
|-----------|-------|-------------|---------------|--------------|-----------------------|------------|--------------|
|AGINCOURT JUNIOR PUBLIC SCHOOL|29 Lockie Ave|9239020|9239021|43.788456, -79.281118|January 9, 2019|43.786566, -79.279023|43.787530, -79.279456|

**Notes:** 
* The Google Sheets API do not read any row with empty cells at the beginning or end of the row or just an entire row of empty cells. It will log an error when that happens.
* The script being used reads up to line 180 although the actual data is less than that. This is to anticipate extra schools which might be added into the sheets in the future.

## 5. Airflow
The Airflow is set up to run daily. A bot has to first be set up on pgAdmin to connect to Airflow. Connect to `/etc/airflow` on EC2 to create a dag file which contains the script for Airflow. More information on that can be found on [Credential management](https://www.notion.so/bditto/Automating-Stuff-5440feb635c0474d84ea275c9f72c362#dcb7f4b37eae48cba5c290dee5a6ef68). The Airflow uses PythonOperator and run tasks for each Google Sheet (curently 2018, 2019, 2020, 2021).

**Note:** An empty `__init__.py` file then has to be created to run Airflow. 

