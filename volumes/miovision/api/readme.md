# API Puller

### Overview

The puller can currently grab crosswalk and tmc data from the Miovision API using specified intersections and dates, output a csv file and upload the resulting csv to the database. The puller can support date ranges longer than 48 hours. The output is the same format as existing csv dumps sent by miovision.

Future steps would be to add the raw data to the volumes table and run the aggregation functions.

### API Key

Emailed from Miovision. Keep it secret. Keep it safe.

### Relevant calls

Each of these returns a 1-minute aggregate, maximum 48-hrs of data, with a two-hour lag (the end time for the query cannot be more recent than two-hours before the query).

### Input Parameters

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

### Error responses


```json
[
    {400: "The provided dates were either too far apart (the limit is 48 hours) or too recent (queries are only permitted for data that is at least 2 hours old)."},
    {404: "No intersection was found with the provided IDs."}
]
```

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

To collect data from a custom date range, run `python intersection_tmc.py run_api --flag=1`. The flag will indicate that a custom date range will be specified. The command line will then prompt you to enter the start and end dates to pull data from.


