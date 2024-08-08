<!-- TOC -->

- [Overview](#overview)
  - [Relevant Calls and Outputs](#relevant-calls-and-outputs)
    - [Turning Movement Count (TMC)](#turning-movement-count-tmc)
    - [Turning Movement Count (TMC) Crosswalks](#turning-movement-count-tmc-crosswalks)
    - [Error responses](#error-responses)
  - [Input Files](#input-files)
  - [How to run the api](#how-to-run-the-api)
    - [TMCs (Volumes)](#tmcs-volumes)
    - [Alerts](#alerts)
  - [Classifications](#classifications)
    - [API Classifications](#api-classifications)
    - [Old Classifications (csv dumps and datalink)](#old-classifications-csv-dumps-and-datalink)
  - [PostgreSQL Functions](#postgresql-functions)
  - [Invalid Movements](#invalid-movements)
  - [How the API works](#how-the-api-works)
  - [Repulling data](#repulling-data)
- [Airflow DAGs](#airflow-dags)
  - [**`miovision_pull`**](#miovision_pull)
    - [`check_partitions` TaskGroup](#check_partitions-taskgroup)
    - [`miovision_agg` TaskGroup](#miovision_agg-taskgroup)
    - [`data_checks` TaskGroup](#data_checks-taskgroup)
  - [**`miovision_check`**](#miovision_check)
  - [Notes](#notes)

<!-- /TOC -->

# Overview
This readme contains information on the script used to pull data from the Miovision `intersection_tmc` API and descriptions of the Airflow DAGs which make use of the API scripts and [sql functions](../sql/readme.md#postgresql-functions) to pull, aggregate, and run data quality checks on new.  

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

which is the same format as the `miovision_api.volumes` table, and directly inserts it to `miovision_api.volumes`. The script converts the movements, classifications and intersections given by the API to uids using the same lookup table structure that exists in the `miovision_api` schema.

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

## Input Files

`config.cfg` is required to access the API and the database. It has the following format:

```ini
[API]
key={api key}
[DBSETTINGS]
host={host ip}
dbname=bigdata
user={username}
password={password}
```

## How to run the api

### TMCs (Volumes)

The process to use the API to download volumes data is typically run through the daily [miovision_pull Airflow DAG](../../../dags/miovision_pull.py). However it can also be run through the command line from within the airflow venv (since Airflow Connections are used for database connection and API key). This can be useful when adding new intersections, or when troubleshooting. 

In command prompt, navigate to the folder where the python file is [located](../api/) and run `python3 intersection_tmc.py run-api-cli ...` with various command line options listed below. For example, to download and aggregate data from a custom date range, run `python3 intersection_tmc.py run-api-cli --pull --agg --start_date=YYYY-MM-DD --end_date=YYYY-MM-DD`. The start and end variables will indicate the start and end date to pull data from the api.

**TMC Command Line Options**

|Option|Format|Description|Example|Default|
|-----|-------|-----|-----|-----|
|start_date|YYYY-MM-DD|Specifies the start date to pull data from. Inclusive. |2018-08-01|The previous day|
|end_date|YYYY-MM-DD|Specifies the end date to pull data from. Must be at least 1 day after `start_date` and cannot be a future date. Exclusive. |2018-08-05|Today|
|intersection|integer|Specifies the `intersection_uid` from the `miovision_api.intersections` table to pull data for. Multiple allowed. |12|Pulls data for all intersection|
|pull|BOOLEAN flag|Use flag to run data pull.|--pull|false|
|agg|BOOLEAN flag|Use flag to run data processing.|--agg|false|

`python3 intersection_tmc.py run-api-cli --pull --agg --start_date=2018-08-01 --end_date=2018-08-05 --intersection=10 --intersection=12` is an example with all the options specified:  
- both data pulling and aggregation specified
- multiple days
- multiple, specific intersections

The `--pull` and `--agg` commands allow us to run data pulling and aggregation together or independently, which is useful for when we want to check out the data before doing any processing. For example, when we are [finding valid intersection movements for new intersections](../update_intersections/readme.md#update-miovision_apiintersection_movements).  

### Alerts

Although it it typically run daily through the Airflow DAG [miovision_pull](../../../dags/miovision_pull.py) `pull_alerts` task, you can also pull from the Alerts API using the command line within the airflow venv (since Airflow Connections are used for database connection and API key). This is helpful for backfilling multiple dates at once. An example command is: 
`python3 pull_alert.py run-alerts-api-cli --start_date=2024-06-01 --end_date=2024-07-01`

**Alerts Command Line Options**

|Option|Format|Description|Example|Default|
|-----|-------|-----|-----|-----|
|start_date|YYYY-MM-DD|Specifies the start date to pull data from. Inclusive. |2018-08-01|The previous day|
|end_date|YYYY-MM-DD|Specifies the end date to pull data from. Must be at least 1 day after `start_date` and cannot be a future date. Exclusive. |2018-08-05|Today|

## Classifications

The classification given in the api is different than the ones given in the csv dumps, or the datalink. 
The script will return an error if a classificaiton received from the API does not match any from the below list. 

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

### Old Classifications (csv dumps and datalink)

|classification_uid|classification|location_only|class_type|
|-----|-----|-----|-----|
1|Lights|f|Vehicles
2|Bicycles|f|Cyclists
3|Buses|f||
4|Single-Unit Trucks|f|Vehicles
5|Articulated Trucks|f|Vehicles
6|Pedestrians|t|Pedestrians
7|Bicycles|t|Cyclists

## PostgreSQL Functions

To perform the data processing, the API script calls several Postgres functions in the `miovision_api` schema. More information about these functions and the database tables can be found in the [sql readme](../sql/readme.md). 

## Invalid Movements

The API script also checks for invalid movements by calling the [`miovision_api.find_invalid_movements`](../sql/function/function-find_invalid_movements.sql) PostgreSQL function. This function will evaluate whether the number of invalid movements is above or below 1000 in a single day, and warn the user if it is. The function does not stop the API script with an exception so manual QC would be required if the count is above 1000.  

## How the API works

This flow chart provides a high level overview of the script:

```mermaid
flowchart TB
    A[" `pull_data` called via\ncommand line `run-api-cli`"]
    B[" `pull_data` called via\n`miovision_pull` Airflow DAG\n`pull_data` task"]
    A & B-->pull_data

    subgraph pull_data["`pull_data`" ]
        direction TB

        subgraph get_intersection_info["`get_intersection_info`" ]
            direction LR
            D{Are specific\nintersections\n specified?}
            E[Grabs entire list of intersections\nfrom database]
            F["Grabs specified intersection(s)\nfrom database"]
            D-->|Yes|F
            D-->|No|E            
        end

        exit[Exit if specified intersections are inactive]
        G["Pulls crosswalk data (table_ped) and\nvehicle/cyclist data (table_veh)"]
        H[Reformats data and appends\nit to temp tables]
        J[Checks if EDT -> EST occured\nand if so discards 2nd 1-2AM\nto prevent duplicates]

        subgraph insert_data["`insert_data`"]
            direction LR
            insert[Inserts data into `volumes` table]
            api_log[Updates `api_log`]
            invalid["Alerts if invalid movements\nfound (`find_invalid_movements`)"]
            insert-->api_log-->invalid
        end

        P{Does current iteration\ndate exceed specified\ndate range?}
        get_intersection_info-->exit-->G-->H-->J-->insert_data-->P
        

        Iterate[Iterate to next 6 hour block.]
        L{Was it\nspecified to not\nprocess the data?}

        P-->|Yes|L
        P-->|No|Iterate
        Iterate-->G

        subgraph process_data["`process_data`"]
            direction LR
            gaps["find_gaps\n(unacceptable_gaps)"]-->
            mvt["aggregate_15_min_mvt\n(volumes_15min_mvt)"]-->
            v15["aggregate_15_min\n(volumes_15min)"]-->
            volumes_daily["aggregate_volumes_daily\n(volumes_daily)"]-->
            report_dates["get_report_dates\n(report_dates)"]
        end
        
        L---->|No|process_data
        L-->|Yes|skip[Skip aggregation]
    end
    
    Q[End of Script]
    
    pull_data-->Q
  ```

## Repulling data

The Miovision ETL DAG `miovision_pull` and the command line `run-api-cli` method, both have deletes built in to each insert/aggregation function. This makes both of these methods idempotent and safe to re-run without the need to manually delete data before re-pulling. Both methods also have an optional intersection_uid parameter which allows re-pulling or re-aggregation of a single intersection or a subset of intersections. 

Neither method supports deleting and re-processing data that is not in **daily blocks** (for example we cannot delete and re-pull data from `'2021-05-01 16:00:00'` to `'2021-05-02 23:59:00'`, instead we must do so from `'2021-05-01 00:00:00'` to `'2021-05-03 00:00:00'`).

# Airflow DAGs

This section describes the Airflow DAGs which we use to pull, aggregate, and run data checks on Miovision data. Deprecated DAGs are described in the Archive [here](../archive.md#11-deprecated-airflow-dags).

<!-- miovision_pull_doc_md -->
## **`miovision_pull`**  
This updated Miovision DAG runs daily at 3am. The pull data tasks and subsequent summarization tasks are separated out into individual Python taskflow tasks to enable more fine-grained control from the Airflow UI. An intersection parameter is available in the DAG config to enable the use of a backfill command for a specific intersections via a list of integer intersection_uids.  

### `check_partitions` TaskGroup  
  - `check_annual_partition` checks if date is January 1st and if so runs `create_annual_partitions`.  
  - `create_annual_partitions` contains any partition creates necessary for a new year.  
  - `check_month_partition` checks if date is 1st of any month and if so runs `create_month_partition`.  
  - `create_month_partition` contains any partition creates necessary for a new month.  
 
`pull_miovision` pulls data from the API and inserts into `miovision_api.volumes` using `intersection_tmc.pull_data` function. 
- `pull_alerts` pulls alerts occuring on this day from the API and inserts into [`miovision_api.alerts`](../sql/readme.md#alerts), updating `end_time` of existing alerts.  

### `miovision_agg` TaskGroup
This task group completes various Miovision aggregations.  
- `find_gaps_task` clears and then populates `miovision_api.unacceptable_gaps` using `intersection_tmc.find_gaps` function. 
- `aggregate_15_min_mvt_task` clears and then populates `miovision_api.volumes_15min_mvt` using `intersection_tmc.aggregate_15_min_mvt` function. 
- `aggregate_15_min_task` clears and then populates `miovision_api.volumes_15min` using `intersection_tmc.aggregate_15_min` function. 
- `zero_volume_anomalous_ranges_task` identifies intersection / classification combos with zero volumes and adds/updates `miovision_api.anomalous_ranges` accordingly.
- `aggregate_volumes_daily_task` clears and then populates `miovision_api.volumes_daily` using `intersection_tmc.aggregate_volumes_daily` function.
- `get_report_dates_task` clears and then populates `miovision_api.report_dates` using `intersection_tmc.get_report_dates` function.

`done` signals that downstream `miovision_check` DAG can run.

### `data_checks` TaskGroup
This task group runs various red-card data-checks on Miovision aggregate tables for the current data interval using [`SQLCheckOperatorWithReturnValue`](../../../dags/custom_operators.py). These tasks are not affected by the optional intersection DAG-level param.  
- `wait_for_weather` delays the downstream data check by a few hours until the historical weather is available to add context.  
- `check_row_count` checks the sum of `volume` in `volumes_15min_mvt`, equivalent to the row count of `volumes` table using [this](../../../dags/sql/select-row_count_lookback.sql) generic sql.
- `check_distinct_classification_uid` checks the count of distinct values in `classification_uid` column using [this](../../../dags/sql/select-sensor_id_count_lookback.sql) generic sql.  
<!-- miovision_pull_doc_md -->

<!-- miovision_check_doc_md -->
## **`miovision_check`**

This DAG replaces the old `check_miovision`. It is used to run daily data quality checks on Miovision data that would generally not require the pipeline to be re-run. 

- `starting_point` waits for upstream `miovision_pull` DAG `done` task to run indicating aggregation of new data is completed.  
- `check_distinct_intersection_uid`: Checks the distinct intersection_uid appearing in todays pull compared to those appearing within the last 60 days. Notifies if any intersections are absent today. Uses [this](../../../dags/sql/select-sensor_id_count_lookback.sql) generic sql.
- `check_gaps`: Checks if any intersections had data gaps greater than 4 hours (configurable using `gap_threshold` parameter). Does not identify intersections with no data today. Notifies if any gaps found. Uses [this](../../../dags/sql/create-function-summarize_gaps_data_check.sql) generic sql.  
- `check_if_thursday`: Skips downstream checks if execution date is not a Thursday (sends notification on Friday).
- `check_open_anomalous_ranges`: Checks if any anomalous_range entries exist with non-zero volume in the last 7 days. Notifies if any found. 
<!-- miovision_check_doc_md -->

## Notes

- `miovision_api.volume` table was truncated and re-run after the script was fixed and unique constraint was added to the table. Data from July 1st - Nov 21st, 2019 was inserted into the `miovision_api` schema on Nov 22nd, 2019 whereas the dates followed will be inserted into the table via airflow. 

- In order to incorporate Miovision data into the volume model, miovision data prior to July 2019 was inserted as well. May 1st - June 30th, 2019 data was inserted into the schema on Dec 12th, 2019 whereas that of Jan 1st - Apr 30th, 2019 was inserted on Dec 13th, 2019. Therefore, **the `volume_uid` in the table might not be in the right sequence** based on the `datetime_bin`.

- There are 8 Miovision cameras that got decommissioned on 2020-06-15 and the new ones are installed separately between 2020-06-22 and 2020-06-24. Note that all new intersections data were pulled on 2020-08-05 and the new gap-filling process has been applied to them. The old intersections 15min_tmc and 15min data was deleted and re-aggregated with the new gap-filling process on 2020-08-06.

