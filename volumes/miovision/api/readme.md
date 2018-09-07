# API Puller

## Table of Contents

1. [Overview](#overview)
2. [Input Parameters](#input-parameters)
3. [Relevant calls and Outputs](#relevant-calls-and-outputs)
4. [Error Responses](#error-responses)
5. [Input Files](#input-files)
6. [How to run the API](#how-to-run-the-api)
7. [Classifications](#classifications)
8. [Invalid Movements](#invalid-movements)
9. [How the API works](#how-the-api-works)
10. [Future Work](#future-work)

### Overview

The puller can currently grab crosswalk and TMC data from the Miovision API using specified intersections and dates, upload the results to the database and aggregates data to 15 minute bins. The puller can support date ranges longer than 48 hours. The output is the same format as existing csv dumps sent by miovision. This is a future alternative to the existing csv dumps and avoids having to continuously go through miovision to get the data. This will create a continuous stream of data that is populated at all time periods and all dates, similar to the bluetooth data.

### Input Parameters

#### API Key and URL

Emailed from Miovision. Keep it secret. Keep it safe.

The API can be accessed at [https://api.miovision.com/intersections/](https://api.miovision.com/intersections/). The general structure is the `base url+intersection id+tmc or crosswalk endpoint`. Additional documentation can be found in here: [http://beta.docs.api.miovision.com/](http://beta.docs.api.miovision.com/)

### Relevant Calls and Outputs

Each of these returns a 1-minute aggregate, maximum 48-hrs of data, with a two-hour lag (the end time for the query cannot be more recent than two-hours before the query). If the volume is 0, the 1 minute bin will not be populated. 

#### Turning Movement Count (TMC)

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

#### Turning Movement Count (TMC) Crosswalks

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

**study\_id**|**study\_name**|**lat**|**lng**|**datetime\_bin**|**classification**|**entry\_dir\_name**|**entry\_name**|**exit\_dir\_name**|**exit\_name**|**movement**|**volume**|
:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|:-----:|
 ||King / University|43.647653|-79.384844|2018-04-20 17:22:00+00|WorkVan|S| |N| |thru|1|
 ||King / University|43.647653|-79.384844|2018-04-20 17:22:00+00|Bus|W| |E| |thru|1|
 ||King / University|43.647653|-79.384844|2018-04-20 17:22:00+00|Bus|E| |W| |thru|1|
| |King / University|43.647653|-79.384844|2018-04-20 17:23:00+00|Light|W| |E| |thru|3|
 ||King / University|43.647653|-79.384844|2018-04-20 17:23:00+00|Light|S| |E| |right|1|
 ||King / University|43.647653|-79.384844|2018-04-20 17:23:00+00|Light|S| |N| |thru|9|
 ||King / University|43.647653|-79.384844|2018-04-20 17:23:00+00|Light|N| |E| |left|1|
 ||King / University|43.647653|-79.384844|2018-04-20 17:23:00+00|Light|N| |S| |thru|12|
 ||King / University|43.647653|-79.384844|2018-04-20 17:23:00+00|Light|E| |N| |right|1|
 ||King / University|43.647653|-79.384844|2018-04-20 17:24:00+00|Light|S| |N| |thru|11|

which is somewhat similar to the csv dumps given from miovision. The goal of the api is to match the formatting as much as possible. 

### Error responses


```json
[
    {400: "The provided dates were either too far apart (the limit is 48 hours) or too recent (queries are only permitted for data that is at least 2 hours old)."},
    {404: "No intersection was found with the provided IDs."}
]
```

There is also a currently unkown `504` error. The script has measures (which have not been tested) to handle this error, but if the data cannot be pulled, retrying will successfully pull the data. The script has the capapbility to pull specific intersections.

### Input Files

|File|Description|
|-----|-----|
`intersection_id.csv`| Lists all intersection names, their intersection IDs to input into the puller, and the lat/lng.
`config.cfg`|Configuration file that contains database credentials and the api key.

`config.cfg` has the following format:

```
[API]
key=your api key
[DBSETTINGS]
host=10.160.12.47
dbname=bigdata
user=database username
password=database password
```

### How to run the api

In command prompt, navigate to the folder where the python file is located and run `python intersection_tmc.py run_api`. This will collect data from the previous day as the default date range.

The script can also customize the data it pulls and processes with various command line options.

For example, to collect data from a custom date range, run `python intersection_tmc.py run_api --start=YYYY-MM-DD --end=YYYY-MM-DD`. The start and end variables will indicate the start and end date to pull data from the api.

`start` and `end` must be separated by at least 1 day, and `end` cannot be a future date. Specifying `end` as today will mean the script will pull data until the start of today (Midnight, 00:00). 

#### Command Line Options

|Option|Format|Description|Example|Default|
|-----|-------|-----|-----|-----|
|start|YYYY-MM-DD|Specifies the start date to pull data from|2018-08-01|The previous day|
|end|YYYY-MM-DD|Specifies the end date to pull data from|2018-08-05|Today|
|intersection|integer|Specifies the `intersection_uid` from the `miovision.intersections` table to pull data for|12|Pulls data for all intersection|
|path|path|Specifies the directory where the `config.cfg` file is|`C:\Users\rliu4\Documents\GitHub\bdit_data-sources\volumes\miovision\api`|`config.cfg` is located in the same directory as the `intersection_tmc.py` file.|
|pull|string|Specifies if the script should only pull data and not process the data|Yes|Processes data in PostgreSQL

`python intersection_tmc.py --start=2018-08-01 --end=2018-08-05 --intersection=12 --path=C:\Users\rliu4\Documents\GitHub\bdit_data-sources\volumes\miovision\api --pull=Yes` is an example with all the options specified.

### Classifications

The classification given in the api is different than the ones given in the csv dumps, or the datalink. 

#### Exisiting Classification (csv dumps and datalink)

|classification_uid|classification|location_only|class_type|
|-----|-----|-----|-----|
1|Lights|f|Vehicles
2|Bicycles|f|Cyclists
3|Buses|f||
4|Single-Unit Trucks|f|Vehicles
5|Articulated Trucks|f|Vehicles
6|Pedestrians|t|Pedestrians
7|Bicycles|t|Cyclists

#### API Classifications

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

### Invalid Movements

The API also checks for invalid movements by calling the [`miovision_api.find_invalid_movements`](https://github.com/CityofToronto/bdit_data-sources/blob/automate_miovision_aggregation/volumes/miovision/sql/function-find_invalid_movements.sql) PostgreSQL function. This function will evaluate whether the number of invalid movements is above or below 1000 in a single day, and warn the user if it is. The function does not stop the API script with an exception so manual QC would be required if the count is above 1000.

### How the API works

This flow chart provides a high level overview:


![Flow Chart of the API](img/api_script1.png)


### Future Work

* Add email notification in the event of an error
