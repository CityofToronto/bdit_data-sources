# API Puller

## Table of Contents

- [API Puller](#api-puller)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Input Parameters](#input-parameters)
    - [API Key and URL](#api-key-and-url)
  - [Relevant Calls and Outputs](#relevant-calls-and-outputs)
    - [Turning Movement Count (TMC)](#turning-movement-count-tmc)
    - [Turning Movement Count (TMC) Crosswalks](#turning-movement-count-tmc-crosswalks)
    - [Error responses](#error-responses)
  - [Input Files](#input-files)
  - [How to run the api](#how-to-run-the-api)
    - [Virtual Environment](#virtual-environment)
    - [Command Line Options](#command-line-options)
  - [Classifications](#classifications)
    - [Exisiting Classification (csv dumps and datalink)](#exisiting-classification-csv-dumps-and-datalink)
    - [API Classifications](#api-classifications)
  - [PostgreSQL Functions](#postgresql-functions)
    - [Aggregation Functions](#aggregation-functions)
    - [Clear Functions](#clear-functions)
    - [Helper Functions](#helper-functions)
    - [Partitioning Functions](#partitioning-functions)
    - [Deprecated Functions](#deprecated-functions)
  - [Invalid Movements](#invalid-movements)
  - [How the API works](#how-the-api-works)
  - [Airflow](#airflow)
    - [**`miovision_pull`**](#miovision_pull)
      - [`check_partitions` TaskGroup](#check_partitions-taskgroup)
      - [`miovision_agg` TaskGroup](#miovision_agg-taskgroup)
      - [`data_checks` TaskGroup](#data_checks-taskgroup)
    - [**`miovision_check`**](#miovision_check)
  - [Airflow - Deprecated](#airflow---deprecated)
    - [**`pull_miovision`**](#pull_miovision)
    - [**`check_miovision`**](#check_miovision)
  - [Notes](#notes)

## Overview

The puller can currently grab crosswalk and TMC data from the Miovision API using specified intersections and dates, upload the results to the database and aggregates data to 15 minute bins. The puller can support date ranges longer than 24 hours. The output is the same format as existing csv dumps sent by miovision. This script creates a continuous stream of volume data from Miovision.

## Input Parameters

### API Key and URL

Emailed from Miovision. Keep it secret. Keep it safe.

The API can be accessed at [https://api.miovision.com/intersections/](https://api.miovision.com/intersections/). The general structure is the `base url+intersection id+tmc or crosswalk endpoint`. Additional documentation can be found in here: [http://beta.docs.api.miovision.com/](http://beta.docs.api.miovision.com/) .

## Relevant Calls and Outputs

Each of these returns a 1-minute aggregate, maximum 48-hrs of data, with a two-hour lag (the end time for the query cannot be more recent than two-hours before the query). If the volume is 0, the 1 minute bin will not be populated. 

### Turning Movement Count (TMC)

Every movement through the intersection except for pedestrians.

Response:

```json
[
  {
    "class": {'type': "string", 'desc': "Class of vehicle/bike"},
    "entrance": {'type':"string", 'desc': "Entrance leg, e.g. 'N'"},
    "exit": {'type':"string",'desc': "Exit leg, e.g. 'W'"},
    "qty": {'type':"int", 'desc': "Count of this movement/class combination"}
  }
]
```

### Turning Movement Count (TMC) Crosswalks

Crosswalk Counts

```json
[
  {
    "class": {'type': "string", 'desc':"They're all pedestrian"},
    "crosswalkSide": {'type':"string", 'desc': "Intersection leg the crosswalk is on"},
    "direction": {'type':"string",'desc': "ClockWise (CW) or CounterCW (CCW)"},
    "qty": {'type':"int", 'desc': "Count"}
  }
]
```

Through the API, the script converts it to a table like this:

**intersection\_uid**|**datetime\_bin**|**classification\_uid**|**leg**|**movement\_uid**|**volume**|
:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|
1|2018-07-03 23:01:00|1|N|1|5
1|2018-07-03 23:03:00|1|N|1|9
1|2018-07-03 23:06:00|1|N|1|7
1|2018-07-03 23:13:00|1|N|1|7
1|2018-07-03 23:28:00|1|N|1|8
1|2018-07-03 23:14:00|1|N|1|5
1|2018-07-03 23:07:00|1|N|1|8
1|2018-07-03 23:15:00|1|N|1|5
1|2018-07-03 23:16:00|1|N|1|3
1|2018-07-03 23:08:00|1|N|1|8

which is the same format as the `miovision.volumes` table, and directly inserts it to `miovision_api.volumes`. The script converts the movements, classifications and intersections given by the API to uids using the same lookup table structure that exists in the `miovision` schema.

### Error responses

These errors are a result of invalid inputs to the API, or the API having an internal error of some kind. 

```json
[
    {400: "The provided dates were either too far apart (the limit is 48 hours) or too recent (queries are only permitted for data that is at least 2 hours old)."},
    {404: "No intersection was found with the provided IDs."}
]
```

There is also a currently unkown `504` error. The script has measures to handle this error, but if the data cannot be pulled, retrying will successfully pull the data. The script has the capapbility to individually pull specific intersections.

There are other errors relating to inserting/processing the data on PostgreSQL and requesting the data. Instead of an error code, details about these kinds of errors are usually found in the logs and in the traceback. 

All errors the API encounters are logged in the `logging.log` file, and emailed to the user. The log also logs the time each intersection is pulled, and the time when each task in PostgreSQL is completed. This makes it useful to check if any single processes is causing delays. 

## Input Files

`config.cfg` is required to access the API, the database, and perform email notification. It has the following format:

```ini
[API]
key=your api key
[DBSETTINGS]
host=10.160.12.47
dbname=bigdata
user=database username
password=database password
[EMAIL]
from=from@email.com
to=to@email.com
```

## How to run the api

In command prompt, navigate to the folder where the python file is located and run `python intersection_tmc.py run_api`. This will collect data from the previous day as the default date range.

The script can also customize the data it pulls and processes with various command line options.

For example, to collect data from a custom date range, run `python intersection_tmc.py run_api --start_date=YYYY-MM-DD --end_date=YYYY-MM-DD`. The start and end variables will indicate the start and end date to pull data from the api.

`start_date` and `end_date` must be separated by at least 1 day, and `end_date` cannot be a future date. Specifying `end` as today will mean the script will pull data until the start of today (Midnight, 00:00). 

If the API runs into an error, an email will be sent notifying the general error category along with the traceback. The logging file also logs the error. For some minor errors that can be fixed by repulling the data, the API will email which intersection-date combination could not be pulled. 

### Virtual Environment

If the script is running on the EC2, then a virtual environment is required to run the script. In addition, it is no longer necessary to specify the proxy since the script is being run outside the firewall. 

First, install pipenv by running `pip install --user pipenv`. The script is written around python 3.x, so run `pipenv --three` to setup a python 3.x virtual environment. To install the required packages such as the click module, run `pipenv intstall click`. 

To run the python script, or any python commands, replace `python` at the start of your command with `pipenv run python`. For example, to run this script, run `pipenv run python intersection_tmc.py run_api`.

More information can be found [here](https://python-docs.readthedocs.io/en/latest/dev/virtualenvs.html).

### Command Line Options

|Option|Format|Description|Example|Default|
|-----|-------|-----|-----|-----|
|start_date|YYYY-MM-DD|Specifies the start date to pull data from|2018-08-01|The previous day|
|end_date|YYYY-MM-DD|Specifies the end date to pull data from|2018-08-05|Today|
|intersection|integer|Specifies the `intersection_uid` from the `miovision.intersections` table to pull data for. Multiple allowed. |12|Pulls data for all intersection|
|path|path|Specifies the directory where the `config.cfg` file is|`/etc/airflow/data_scripts/volumes/miovision/api/config.cfg`|`config.cfg` is located in the same directory as the `intersection_tmc.py` file.|
|pull|BOOLEAN flag|Data processing and gap finding will be skipped|--pull|false|

`python3 intersection_tmc.py run-api --start_date=2018-08-01 --end_date=2018-08-05 --intersection=10 --intersection=12 --path=/etc/airflow/data_scripts/volumes/miovision/api/config.cfg --pull` is an example with all the options specified. However, the usual command line that we run daily is `python3 intersection_tmc.py run-api --path=/etc/airflow/data_scripts/volumes/miovision/api/config.cfg` since we are only interested in a day worth of data on the day before on ALL working intersections and we want data processing to happen.  

If `--pull` is specified in the command line (which is equivalent to setting it to True), the script will skip the data processing and gaps finding process. This is useful when we want to just insert data into the volumes table and check out the data before doing any processing. For example, when we are [finding valid intersection movements for new intersections](https://github.com/CityofToronto/bdit_data-sources/tree/miovision_api_bugfix/volumes/miovision#4-steps-to-add-or-remove-intersections).

## Classifications

The classification given in the api is different than the ones given in the csv dumps, or the datalink. 

### Exisiting Classification (csv dumps and datalink)

|classification_uid|classification|location_only|class_type|
|-----|-----|-----|-----|
1|Lights|f|Vehicles
2|Bicycles|f|Cyclists
3|Buses|f||
4|Single-Unit Trucks|f|Vehicles
5|Articulated Trucks|f|Vehicles
6|Pedestrians|t|Pedestrians
7|Bicycles|t|Cyclists

### API Classifications

|classification_uid|classification|location_only|class_type|
|-----|-----|-----|-----|
1|Light|f|Vehicles
2|Bicycle|f|Cyclists
3|Bus|f||
4|SingleUnitTruck|f|Vehicles
5|ArticulatedTruck|f|Vehicles"
6|Pedestrian|t|Pedestrians"
8|WorkVan|f|Vehicles"

The API assigns a classification of 0 if the classification matches none of the above. This is possible if the classification given from the API does not exactly match any of the ones in the script. One example is if `Light` is pluralized as `Lights`, like in the CSV dumps.

## PostgreSQL Functions

To perform the data processing, the API script calls several postgres functions in the `miovision_api` schema. These functions are the same/very similar to the ones used to process the csv dumps. More information can be found in the [miovision readme](../README.md)

### Aggregation Functions  

| Edited | Function | Comment |
|---|---|---|
| x | [`aggregate_15_min(start_date date, end_date date, intersections integer[])`](../sql/function/function-aggregate-volumes_15min.sql) | Aggregates data from `miovision_api.volumes_15min_mvt` (turning movements counts/TMC) into `miovision_api.volumes_15min` (automatic traffic recorder /ATR). Also updates `miovision_api.volumes_mvt_atr_xover` and `miovision_api.volumes_15min_mvt.processed` column. Takes an optional intersection array parameter to aggregate only specific intersections. Use `clear_volumes_15min()` to remove existing values before summarizing. |
| x | [`aggregate_15_min_mvt(start_date date, end_date date)`](../sql/function/function-aggregate-volumes_15min_mvt.sql) | Aggregates valid movements from `miovision_api.volumes` in to `miovision_api.volumes_15min_mvt` as 15 minute turning movement counts (TMC) bins and fills in gaps with 0-volume bins. Also updates foreign key in `miovision_api.volumes`. Takes an optional intersection array parameter to aggregate only specific intersections. Use `clear_15_min_mvt()` to remove existing values before summarizing. |
| x | [`aggregate_volumes_daily(start_date date, end_date date)`](../sql/function/function-aggregate-volumes_daily.sql) | Aggregates data from `miovision_api.volumes_15min_mvt` into `miovision_api.volumes_daily`. Includes a delete clause to clear the table for those dates before any inserts. |
| x | [`api_log(start_date date, end_date date, intersections integer[])`](../sql/function/function-api_log.sql) | Logs inserts from the api to miovision_api.volumes via the `miovision_api.api_log` table. Takes an optional intersection array parameter to aggregate only specific intersections. Use `clear_api_log()` to remove existing values before summarizing. |
| x | [`get_report_dates(start_date timestamp, end_date timestamp, intersections integer[])`](../sql/function/function-get_report_dates.sql) | Logs the intersections/classes/dates added to `miovision_api.volumes_15min` to `miovision_api.report_dates`. Takes an optional intersection array parameter to aggregate only specific intersections. Use `clear_report_dates()` to remove existing values before summarizing. |
|  | [`find_gaps(start_date date, end_date date)`](../sql/function/function-find_gaps.sql) | Find unacceptable gaps and insert into table `miovision_api.unacceptable_gaps`. |  

### Clear Functions  

| Edited | Function | Comment |
|---|---|---|
|  | [`clear_15_min_mvt(start_date timestamp, end_date timestamp, intersections integer[])`](../sql/function/function-clear-volumes_15min_mvt.sql) | Clears data from `miovision_api.volumes_15min_mvt` in order to facilitate re-pulling. `intersections` param defaults to all intersections. |
|  | [`clear_api_log(_start_date date, _end_date date, intersections integer[])`](../sql/function/function-clear-api_log.sql) | Clears data from `miovision_api.api_log` in order to facilitate re-pulling. `intersections` param defaults to all intersections. |
|  | [`clear_report_dates(_start_date date, _end_date date, intersections integer[])`](../sql/function/function-clear-report_dates.sql) | Clears data from `miovision_api.report_dates` in order to facilitate re-pulling. `intersections` param defaults to all intersections. |
|  | [`clear_volumes(start_date timestamp, end_date timestamp, intersections integer[])`](../sql/function/function-clear-volumes.sql) | Clears data from `miovision_api.volumes` in order to facilitate re-pulling. `intersections` param defaults to all intersections. |
| x | [`clear_volumes_15min(start_date timestamp, end_date timestamp, intersections integer[])`](../sql/function/function-clear-volumes_15min.sql) | Clears data from `miovision_api.volumes_15min` in order to facilitate re-pulling. `intersections` param defaults to all intersections. |  

### Helper Functions

| Edited | Function | Comment |
|---|---|---|
| x | [`find_invalid_movements(start_date timestamp, end_date timestamp)`](../sql/function/function-find_invalid_movements.sql) | Used exclusively within `intersection_tmc.py` `insert_data` function to raise notice in the logs about invalid movements. |
| x | [`get_intersections_uids(intersections integer[])`](../sql/function/function-get_intersection_uids.sql) | Returns all intersection_uids if optional `intersections` param is omitted, otherwise returns only the intersection_uids provided as an integer array to intersections param. Used in `miovision_api.clear_*` functions. Example usage: `SELECT miovision_api.get_intersections_uids() --returns all intersection_uids` or `SELECT miovision_api.get_intersections_uids(ARRAY[1,2,3]::integer[]) --returns only {1,2,3}` |  

### Partitioning Functions  

| Edited | Function | Comment |
|---|---|---|
|  | [`create_mm_nested_volumes_partitions(base_table text, year_ integer, mm_ integer)`](../sql/function/function-create_mm_nested_volumes_partitions.sql) | Create a new month partition under the parent year table `base_table`. Only to be used for miovision_api `volumes_15min` and `volumes_15min_mvt` tables. Example: `SELECT miovision_api.create_yyyy_volumes_partition('volumes_15min', 2023)` |
|  | [`create_yyyy_volumes_15min_partition(base_table text, year_ integer)`](../sql/function/function-create_yyyy_volumes_15min_partition.sql) | Create a new year partition under the parent table `base_table`. Only to be used for miovision_api `volumes_15min` and `volumes_15min_mvt` tables. Example: `SELECT miovision_api.create_yyyy_volumes_partition('volumes_15min', 2023)` |
|  | [`create_yyyy_volumes_partition(base_table text, year_ integer, datetime_col text)`](../sql/function/function-create_yyyy_volumes_partition.sql) | Create a new year partition under the parent table `base_table`. Only to be used for miovision_api `volumes` table. Use parameter `datetime_col` to specify the partitioning timestamp column, ie. `datetime_bin`. Example: `SELECT miovision_api.create_yyyy_volumes_partition('volumes', 2023, 'datetime_bin')` |  

### Deprecated Functions

| Edited | Function | Comment |
|---|---|---|
| x | [`determine_working_machine(start_date date, end_date date)`](../sql/function/function-determine_working_machine.sql) | Function no longer in use. Previously used in `check_miovision` DAG to determine if any cameras had gaps larger than 4 hours. See: `miovision_check` DAG `check_gaps` task for new implementation.  |
| x | `missing_dates(_date date)` | Function no longer in use. Previously used to log dates with missing data to `miovision_api.missing_dates`. |  

## Invalid Movements

The API also checks for invalid movements by calling the [`miovision_api.find_invalid_movements`](../sql/function/function-find_invalid_movements.sql) PostgreSQL function. This function will evaluate whether the number of invalid movements is above or below 1000 in a single day, and warn the user if it is. The function does not stop the API script with an exception so manual QC would be required if the count is above 1000.  

## How the API works

This flow chart provides a high level overview of the script:
![Flow Chart of the API](img/api_script1.png)

Below shows an overview of functions used in the script:
![Python Functions](img/python_functions.png)

Below shows a list of tables used (separated by source table and results table):
![Source Tables](img/tables_1.png)
![Results Tables](img/tables_2.png)

Below shows a list of SQL functions used:
![SQL Functions](img/functions.png)

Below shows a list of SQL trigger functions, materialized view and sequences used:
![Trigger Functions and Sequences](img/others.png)

## Airflow

<!-- miovision_pull_doc_md -->
### **`miovision_pull`**  
This updated Miovision DAG runs daily at 3am. The pull data tasks and subsequent summarization tasks are separated out into individual Python taskflow tasks to enable more fine-grained control from the Airflow UI. An intersection parameter is available in the DAG config to enable the use of a backfill command for a specific intersections via a list of integer intersection_uids.  

#### `check_partitions` TaskGroup  
  - `check_month_partition` checks if date is 1st of any month and if so runs `create_month_partition`. 
  - `check_annual_partition` checks if date is January 1st and if so runs `create_annual_partitions`. 
  - `create_annual_partitions` contains any partition creates necessary for a new year.
  - `create_month_partition` contains any partition creates necessary for a new month.

 
`pull_miovision` pulls data from the API and inserts into `miovision_api.volumes` using `intersection_tmc.pull_data` function. 

#### `miovision_agg` TaskGroup
This task group completes various Miovision aggregations.  
- `find_gaps_task` clears and then populates `miovision_api.unacceptable_gaps` using `intersection_tmc.find_gaps` function. 
- `aggregate_15_min_mvt_task` clears and then populates `miovision_api.volumes_15min_mvt` using `intersection_tmc.aggregate_15_min_mvt` function. 
- `aggregate_15_min_task` clears and then populates `miovision_api.volumes_15min` using `intersection_tmc.aggregate_15_min` function. 
- `aggregate_volumes_daily_task` clears and then populates `miovision_api.volumes_daily` using `intersection_tmc.aggregate_volumes_daily` function.
- `get_report_dates_task` clears and then populates `miovision_api.report_dates` using `intersection_tmc.get_report_dates` function.

`done` signals that downstream `miovision_check` DAG can run.

#### `data_checks` TaskGroup
This task group runs various red-card data-checks on Miovision aggregate tables for the current data interval using [`SQLCheckOperatorWithReturnValue`](../../../dags/custom_operators.py). These tasks are not affected by the optional intersection DAG-level param. 
 tasks perform checks on the aggregated data from the current data interval.  
- `check_row_count` checks the sum of `volume` in `volumes_15min_mvt`, equivalent to the row count of `volumes` table using [this](../../../dags/sql/select-row_count_lookback.sql) generic sql.
- `check_distinct_classification_uid` checks the count of distinct values in `classification_uid` column using [this](../../../dags/sql/select-sensor_id_count_lookback.sql) generic sql.
<!-- miovision_pull_doc_md -->

<!-- miovision_check_doc_md -->
### **`miovision_check`**

This DAG replaces the old `check_miovision`. It is used to run daily data quality checks on Miovision data that would generally not require the pipeline to be re-run. 

- `starting_point` waits for upstream `miovision_pull` DAG `done` task to run indicating aggregation of new data is completed.  
- `check_distinct_intersection_uid`: Checks the distinct intersection_uid appearing in todays pull compared to those appearing within the last 60 days. Notifies if any intersections are absent today. Uses [this](../../../dags/sql/select-sensor_id_count_lookback.sql) generic sql.
- `check_gaps`: Checks if any intersections had data gaps greater than 4 hours (configurable using `gap_threshold` parameter). Does not identify intersections with no data today. Notifies if any gaps found. Uses [this](../../../dags/sql/create-function-summarize_gaps_data_check.sql) generic sql.
<!-- miovision_check_doc_md -->

## Airflow - Deprecated

<!-- pull_miovision_doc_md -->
### **`pull_miovision`**  
This deprecated Miovisiong DAG (replaced by [`miovision_pull`](#miovision_pull)) uses a single BashOperator to run the entire data pull and aggregation in one task.  
The BashOperator runs one task named `pull_miovision` using a bash command that looks something like this bash_command = `'/etc/airflow/.../intersection_tmc.py run-api --path /etc/airflow/.../config.cfg'`. 
<!-- pull_miovision_doc_md -->

<!-- check_miovision_doc_md -->
### **`check_miovision`**

The `check_miovision` DAG is deprecated by the addition of the [`data_checks` TaskGroup](#data_checks-taskgroup) to the main `miovision_pull` DAG (and `miovision_check` DAG), in particular `miovision_check.check_gaps` which directly replaces `check_miovision.check_miovision`.  
This DAG previously was used to check if any Miovision camera had a gap of at least 4 hours. More information can be found at [this part of the README.](https://github.com/CityofToronto/bdit_data-sources/tree/miovision_api_bugfix/volumes/miovision#3-finding-gaps-and-malfunctioning-camera)
<!-- check_miovision_doc_md -->

## Notes

- `miovision_api.volume` table was truncated and re-run after the script was fixed and unique constraint was added to the table. Data from July 1st - Nov 21st, 2019 was inserted into the `miovision_api` schema on Nov 22nd, 2019 whereas the dates followed will be inserted into the table via airflow. 

- In order to incorporate Miovision data into the volume model, miovision data prior to July 2019 was inserted as well. May 1st - June 30th, 2019 data was inserted into the schema on Dec 12th, 2019 whereas that of Jan 1st - Apr 30th, 2019 was inserted on Dec 13th, 2019. Therefore, **the `volume_uid` in the table might not be in the right sequence** based on the `datetime_bin`.

- There are 8 Miovision cameras that got decommissioned on 2020-06-15 and the new ones are installed separately between 2020-06-22 and 2020-06-24. Note that all new intersections data were pulled on 2020-08-05 and the new gap-filling process has been applied to them. The old intersections 15min_tmc and 15min data was deleted and re-aggregated with the new gap-filling process on 2020-08-06.
