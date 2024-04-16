
# *The RESCU schema is now deprecated. Please refer to [`vds` schema](../vds/readme.md).*
- Use `vds.counts_15min` instead of `rescu.volumes_15min`
- Identify RESCU sensors in the new schema using `vds.detector_inventory WHERE det_type = 'RESCU Detectors'`

<br>  
<br>  
<br>  
<br>
<br>  

# ~~RESCU Vehicle Detector Stations~~ <!-- omit in toc -->

Road Emergency Services Communication Unit (RESCU) data tracks traffic volume on expressways using loop detectors. 

- [Keywords](#keywords)
- [Description:](#description)
- [Schema](#schema)
  - [Data Dictionary:](#data-dictionary)
- [What do we use this for:](#what-do-we-use-this-for)
  - [1- What is the purpose of using the data? (Primary or secondary)](#1--what-is-the-purpose-of-using-the-data-primary-or-secondary)
  - [2- Who uses this data within Data \& Analytics unit (D\&A)?  What tasks is data used for?](#2--who-uses-this-data-within-data--analytics-unit-da--what-tasks-is-data-used-for)
  - [4- What are the limitations with using this data based the above uses?](#4--what-are-the-limitations-with-using-this-data-based-the-above-uses)
  - [5- Does this data get published?](#5--does-this-data-get-published)
- [Date of data collection:](#date-of-data-collection)
- [Data Ownership](#data-ownership)
  - [1- Who is responsible for this data within D\&A?](#1--who-is-responsible-for-this-data-within-da)
  - [2- Who is responsible for this data outside D\&A?](#2--who-is-responsible-for-this-data-outside-da)
- [METHODOLOGICAL INFORMATION](#methodological-information)
  - [Description of methods used for collection/generation of data:](#description-of-methods-used-for-collectiongeneration-of-data)
    - [1- How is the data collected?](#1--how-is-the-data-collected)
    - [2- How often is the data updated?](#2--how-often-is-the-data-updated)
  - [Methods for processing the data: How does the data move/transform through the organization?](#methods-for-processing-the-data-how-does-the-data-movetransform-through-the-organization)
    - [1- How is the data organized and aggregated?](#1--how-is-the-data-organized-and-aggregated)
    - [2- Where is the raw data stored?](#2--where-is-the-raw-data-stored)
    - [3- How is the raw data stored?](#3--how-is-the-raw-data-stored)
    - [4- Where is the data stored?](#4--where-is-the-data-stored)
- [Data Quality/ Describe any quality-assurance procedures performed on the data](#data-quality-describe-any-quality-assurance-procedures-performed-on-the-data)
  - [1- Are there known data gaps/incomplete data?](#1--are-there-known-data-gapsincomplete-data)
  - [2- What are the gaps?](#2--what-are-the-gaps)
  - [3- How are data gaps/incomplete data addressed?](#3--how-are-data-gapsincomplete-data-addressed)
  - [4- Who is responsible for addressing data gaps/incomplete data?](#4--who-is-responsible-for-addressing-data-gapsincomplete-data)
  - [5- Are there data quality assessment processes for the data?](#5--are-there-data-quality-assessment-processes-for-the-data)
  - [6- How often are data quality assessment processes for the data undertaken?](#6--how-often-are-data-quality-assessment-processes-for-the-data-undertaken)
- [Data Maintenance](#data-maintenance)
  - [1- Who is responsible for the status of data functionality and the overall maintenance of the data collection?](#1--who-is-responsible-for-the-status-of-data-functionality-and-the-overall-maintenance-of-the-data-collection)
  - [2- Who should be notified if something goes wrong/ there are changes to data?](#2--who-should-be-notified-if-something-goes-wrong-there-are-changes-to-data)
- [How the data are loaded](#how-the-data-are-loaded)
  - [`rescu_pull.py`](#rescu_pullpy)
  - [`check_rescu.py`](#check_rescupy)

## Keywords

RESCU, traffic, volume, long_term, Data and Analytics

## Description:

The City's Road Emergency Services Communication Unit (RESCU) tracks and manages traffic volume on expressways and some arterial roads using various technologies. Within D&A, only the loop detector portion of the broader RESCU system is used. General information can be found [here](https://en.wikipedia.org/wiki/Road_Emergency_Services_Communications_Unit). 


## Schema

- Data is stored in the `rescu` schema.
- The main tables are listed below. Please note that `rescu.volumes_15min` and `rescu.detector_inventory` are the tables that should be used for querying.

### Data Dictionary:

`rescu.detector_inventory` table: 
This table contains details of RESCU VDS in the City. Its origin has been lost to time and it is not being automatically updated, so details may be out of date
.
Arterycode can be used used to join the data with `traffic.artery_data` 

| column_name      | data_type         | sample                | Description  |
|:-----------------|:------------------|:----------------------|--------------|
| detector_id      | text              | DW0040DEL             | You can quickly tell the direction via the second last letter. 
| number_of_lanes  | smallint          | 3                     |
| latitude         | numeric           | 43.635944             |
| longitude        | numeric           | -79.401186            |
| det_group        | text              | LAKE                  | "ALLEN" (Allen road), "FGG/LAKE" (Gardiner Expressway and Lakeshore ramps) , "FGG" (Gardiner Expressway), "DVP" (Don Valley Parkway), "LAKE" (Lakeshore)
| road_class       | text              | Major Arterial        |
| primary_road     | text              | Lake Shore Blvd W     |
| direction        | character varying | E                     |
| offset_distance  | integer           |                       |
| offset_direction | text              | W of                  |
| cross_road       | text              | BATHURST STREET       |
| district         | text              | Toronto and East York |
| ward             | text              | Trinity-Spadina (20)  |
| vds_type         | text              | inductive_loop        |
| total_loops      | smallint          | 6                     |
| sequence_number  | text              | LAKE173               |
| data_range_low   | integer           | 20000                 |
| data_range_high  | integer           | 30000                 |
| historical_count | integer           | 9700                  |
| arterycode       | integer           | 826                   | Reference table for Artery Codes (internal reference for intersections and segments)

`rescu.raw_20sec` table: the table includes the raw number of counts recorded by the detector during that 20-second interval. **This table only has data for 7 days, the last being 2020-04-08.**

| column_name   | data_type                   | sample              |
|:--------------|:----------------------------|:--------------------|
| datetime_bin  | timestamp without time zone | 2020-04-08 00:00:00 |
| detector_id   | text                        | DE0010DEG           |
| lane_no       | integer                     | 1                   |
| volume        | integer                     | 0                   |
| occupancy     | numeric                     | 0.0                 |
| speed         | numeric                     | 0.0                 |
| uid           | integer                     | 1                   |

`rescu.raw_15min` table: The table includes aggregated raw counts over 15 min interval. This table is used to insert rows into `rescu.volumes_15min` via [`create-trigger-function-populate_volumes_15min.sql`](create-trigger-function-populate_volumes_15min.sql).

rescu.raw_15min
| column_name   | data_type   | sample                     | Description |
|:--------------|:------------|:---------------------------|-------------|
| dt            | date        | 2020-01-13                 |
| raw_info      | text        | 0000 - 0015 de0010der   -1 | Format "hhmm - hhmm detector volume". If volume is -1 that means the detector is down and the row is not inserted into volumes_15min. 
| raw_uid       | integer     | 1                          |

`rescu.volumes_15min` table: This table includes processed 15 min counts.

rescu.volumes_15min
| column_name   | data_type                   | sample              |
|:--------------|:----------------------------|:--------------------|
| detector_id   | text                        | DE0010DER           |
| datetime_bin  | timestamp without time zone | 2019-01-02 00:00:00 |
| volume_15min  | integer                     | 29                  | 
| arterycode    | integer                     | 3272                |
| volume_uid    | integer                     | 5234941             |


## What do we use this for:
	
### 1- What is the purpose of using the data? (Primary or secondary)	

Analytics as fulfilled by data specialists as part of data requests. 
Despite data quality concerns (usually due to malfunctioning detectors) RESCU data are the only source of volume counts for highways within Toronto's jurisdiction (the Allen Expressway (*As at May 2023, there is no data on the Allen Expressway since 2021-10*), the Gardiner Expressway and the Don Valley Parkway).


### 2- Who uses this data within Data & Analytics unit (D&A)?  What tasks is data used for?	

Data specialists and research analysts. Used for data requests (mostly staff asking for volumes on highways).   

<!-- ### 3- Who uses this data outside D&A? 
> TMC. Only used for real time data reporting. -->

### 4- What are the limitations with using this data based the above uses?	

-Data gaps make data unreliable for data requests. 
-In 2022 and 2023 (as of May) we have identified significant network wide outages that span days to weeks meaning data availability is very sparse. 
-There are rare opportunities to repair RESCU detectors, for example: https://www.toronto.ca/services-payments/streets-parking-transportation/road-maintenance/bridges-and-expressways/expressways/gardiner-expressway/gardiner-expressway-maintenance-program/ 

### 5- Does this data get published?	

No. Data could be published but at this current point it is not due to data quality concerns.


## Date of data collection: 

Though data from RESCU detectors has been available since January 2017 and is updated daily, there are many gaps in the data and humans seeking to use this dataset should check the data availability and quality for their required dates and locations.

<!---## **Geographic location of data collection**:
>to be added --->

## Data Ownership
	
### 1- Who is responsible for this data within D&A?

The Data Operations Team. When [this pipeline gets upgraded](https://github.com/CityofToronto/bdit_data-sources/issues/603) there will be a TDS who owns it.

### 2- Who is responsible for this data outside D&A?

Active Traffic Management. As ITS Central is becoming of more use (as field servers are no longer working and ITS Central is the replacement). 



<!---## Support for data collection
>TBA---

<!---# SHARING/ACCESS INFORMATION
## Restrictions placed on the data: 
> list any restrictions on data access (public data, internal data (team, unit, division, the City, a specific group pf people))



## Internal access to the data
> any process to access the data (requires registration, aquire permission, a link, letting someone know)

> provide the information of how internal team (Data & Analytics un) can find the data:
server name, schema, database, version, admin, 



## Licenses placed on the data (Privacy concerns)
> provide the link to thye known licences (e.g. CC)

> if it's not a known licence, list the concerns and limitations due to the licensing



## Limitation of the data
>Data gaps make data unreliable for data requests.



<!---## Links to any use the data: 
>TBA--->



<!---## Links to other publicly accessible locations of the data: 
>TBA



## Links/relationships to supplimentary data sets:
> like centreline for studies/collisions
>TBA




## Do you expect citation for this dataset: 
> TBA--->




<!---# DATA & FILE OVERVIEW
## List files
> if the dataset or a version of it is stored in other places, list them here and provide information versioning information



## Relevant datasets
> if to work with the data, we need to access another dataset/ or there are relevant dataset, list them here with a link (schema/ folder address)



## Format of accessible data
> the final ready format 
> what would be the first and easy way to access disaggregated data (low effort for us)



## Available formats 
> what format the data was collected in (original format)?
> other formats that data is available: map, txt, geojson, excel, shapefile



## Versions of the data
> if yes, 
1. name of file(s) that was updated: 
2. Why was the file updated? 
3. When was the file updated? 

> use a table formatting (version id, created at, changes)
--->


## METHODOLOGICAL INFORMATION

### Description of methods used for collection/generation of data:

#### 1- How is the data collected?	

Loop detectors installed on the ground of the road surface.
Radar detectors (which function the same way as loop detectors) placed on the roadside. 

<!--- ### 2- Is there an up to date map for this data? If yes, who should be updating the map?      
>- A new map by D&A is underway
>- Electrical contractor provides data feed which included latitude and longitude, however there are locational errors. The overall accuracy of the spatial data provided is questionable. 
>- TPIM (Traffic Plant / Installation and Maintenance) would be responsible for updating this map as it falls within their purview. --->

#### 2- How often is the data updated?	

Raw data is fed into Oracle every 20 seconds. These volume counts are aggregated to 15 minute bins and stored in the rescu schema of the postgres bigdata database via an airflow pipeline on a daily basis. You can read the python script [here](rescu_pull.py).

### Methods for processing the data: How does the data move/transform through the organization?
	
#### 1- How is the data organized and aggregated? 	

Raw data is recorded in 20 second increments. Data handled by D&A is has been pre-aggregated to 15 minute increments through a legacy process developed by ATM. Data is matched to arterycode (directional). The vehicle detector stations have coordinates. 

#### 2- Where is the raw data stored?	   	

Raw data is stored in ITS Central and ATM's Oracle database. 
Within ITS Central it is stored in Postgres database but we are unsure as to how much data is being stored in the system.

#### 3- How is the raw data stored?	

Unsure—particularly now that there are two streams (one in ITS Central and one in Oracle).

#### 4- Where is the data stored?	

Pulling data from databases into Postgres. Windows task scheduler is responsible for pulling the data from `\\tssrv7`. 
Refer to [How the data are loaded](#How the data are loaded) below for more information

<!---### 5- How is the data stored?	
>Unsure.   




## Instrument/device or software-specific information needed to interpret the data:
> Provide information on how data can be interpreted baed on collection device/instrument  



## Standards information, if appropriate: 
> N/A- to be added



## Environmental/experimental conditions: 
> N/A- to be added --->



## Data Quality/ Describe any quality-assurance procedures performed on the data

### 1- Are there known data gaps/incomplete data? 	

Yes, there are many data gaps.

In June 2023 the following queries were developed as part of an investigation into sensors in need of maintenance (Issue #617), which identify outage periods for individual sensors and for the whole network: `MATERIALIZED VIEW gwolofs.rescu_individual_outages` and `VIEW gwolofs.network_outages`. More information on how to use these queries to identify sensors in need of repair can be found [here](./validation/readme.md). 

   **[network_outages](./sql/create-view-network-outages.sql)**  
   Creates a table of network wide RESCU outages (no values from any sensor over any duration). This can be used to find good dates for a data request or to be part of an alert pipeline.  
   For example, here is some sample code to start with a list of eligible dates for a request and exclude any date with a network outage of any length:

```sql
--use case: find dates and times when there were no network outages: 
--there are 34 dates so far in 2023 with no network wide outages. 
WITH list_dates AS (
    SELECT generate_series('2023-01-01', '2023-06-12', '1 day'::interval)::timestamp AS date
)

SELECT l.date 
FROM list_dates AS l
LEFT JOIN gwolofs.network_outages AS nout ON
    nout.date_start <= l.date AND
    nout.date_end >= l.date
WHERE nout.date_start IS NULL
```

   **[rescu_individual_outages](./sql/create-mat-view-individual-outages.sql)**  
   Use to identify individual detector outages. Could be useful for future data requests.  
   Similar to network outages but for each individual detector. Currently network wide and individual outages overlap due to difficulty of separating overlapping datetime ranges.  
   Here is a sample query which finds a list of dates where a list of sensors are all active with no individual outages:

```sql
WITH list_dates AS (
    SELECT generate_series('2023-01-01', '2023-05-15', '1 day'::interval)::timestamp AS date
),

detectors AS (
    SELECT detector_id
    FROM rescu.detector_inventory
    WHERE det_group = 'FGG' --all gardiner sensors
) 

SELECT l.date
FROM list_dates AS l
CROSS JOIN detectors AS d
LEFT JOIN gwolofs.rescu_individual_outages AS iout ON
    iout.detector_id = d.detector_id AND
    iout.date_start <= l.date AND
    iout.date_end >= l.date
WHERE iout.time_start IS NULL
GROUP BY 1
HAVING COUNT(*) = (SELECT COUNT(*) FROM detectors)
```


### 2- What are the gaps?	

Missing volumes due to detector issues. Data reports sent to D&A out of Oracle contain specific lane-level data. This may help isolate detectors that are down and help in closing gaps and validating data.

### 3- How are data gaps/incomplete data addressed? 

Currently within D&A and for the purpose of Data Requests, requesters tell requestees that data is not available.  

### 4- Who is responsible for addressing data gaps/incomplete data?

Gaps are handled / addressed using a variety of strategies, depending on the intended use of the data and the nature of the gap. D&A can be contacted for gaps  handled by them.

### 5- Are there data quality assessment processes for the data?

There is a daily check run in Airflow [`check_rescu.py`](#check_rescupy) to see if a threshold of data points is met - if there are fewer than 7,000 rows, an alert is raised (almost daily as of 2023-05-10).

### 6- How often are data quality assessment processes for the data undertaken? 
D&A process done daily. QA process counts the number of rows that have data with 7000 rows being the threshold.   
 
There have also been analyses completed to check which non-ramp detectors were recording valid data in 2021 using the following methodology:
1. Count the daily bins per detector. Filter out detectors with fewer than 96 15-minute bins in a 24 hour period (since they must be missing data)
2. Calculate daily volume counts for the valid detectors
3. Calculate the median weekday and weekend daily volume count per detector
4. Group the median weekday daily volumes by corridor and graph them
5. Visually determine a minimum threshold based on the graphs.

 The 2021 minimum thresholds were as follows:
 - Allen Expressway - Weekday: 4000 per lane
 - Allen Expressway - Weekend: 3000 per lane
 - Don Valley Parkway - Weekday: 15000 per lane
 - Don Valley Parkway - Weekend: 10000 per lane
 - Gardiner Expressway - Weekday: 10000 per lane
 - Gardiner Expressway - Weekend: 10000 per lane
 - Lakeshore Boulevard - Weekday: 2000 per lane
 - Lakeshore Boulevard - Weekend: 2000 per lane

RESCU data were then extracted for the detectors and dates that met these thresholds.

The code used to complete these checks can be found in the [date_evaluation folder](#date_evaluation).

<!---#### 6- Are external contractors or consultants responsible for checking data quality?
> Unsure. 


<!---## Data Validity- Is the data truly representative of the real world? 
	
### 1- Who is responsible for checking the validity of the data?	
>Unsure. 
It appears that validation is only done once data reaches D&A. Unclear is there are validation processes upstream. 

### 2- Who should be involved when there are inconsistencies identified with the data?	
>Unsure.
### 3- For external data sources: is data received previously validated? 	
>N/A
### 4- For external data sources: if data received is previously validated, how is it validated?
>N/A
### 5- Are external contractors or consultants responsible for checking data validity? Who is the City staff contact who works for that vendor?	
>N/A
--->

## Data Maintenance

### 1- Who is responsible for the status of data functionality and the overall maintenance of the data collection? 	

- Hardware: Traffic Plant / Installation and Maintenance (TPIM) is responsible. 
- ITS Central: 
- Oracle DB Server: The systems solution integrator in Active Traffic Management.

<!---### 2- How often is data maintained? Are there monitoring mechanisms when parts of the data flow is not working?	
>Unsure. 
### 3- Is there a chain/process for when equipment goes wrong/breaks?	
> Unsure.
### 4- What is the process that needs to be undertaken when equipment goes wrong/breaks? 	
> Unsure.--->
### 2- Who should be notified if something goes wrong/ there are changes to data? 	

- Data Operations; and, eventually,
- ATM. 

<!---### 5- Are external contractors or consultants responsible for maintenance? Who is the City staff contact who works for that vendor?	
>ATM is leased to a contractor in the traffic control room. Contractor runs and manages the work orders.--->
 

<!---## Missing data: 
> TBA



## Specialized formats or other abbreviations used:
>TBA

## Other
> if there are information worth sharing but have not considered in this template, please list them here--->

## How the data are loaded

### `rescu_pull.py`

This [script](rescu_pull.py) is located in the terminal server and takes in three variables which are `start_date`, `end_date` (exclusive) and `path`. It is a job that runs on a daily basis in the morning and imports the latest 15-minute volume file from a particular drive into our RDS. By default, the `start_date` and `end_date` are the beginning and the end of the day before (today - 1 day and 12:00 today respectively). However, the user can also specify the date range from which the data should be pulled to ensure that this process can be used for other applications too. This script is automated to run daily on the terminal server to ingest a day worth of data collected from the day before. The steps are as followed:

1) The script reads information from a .rpt file and then inserts the date into a table named `rescu.raw_15min`. The table has the following information

|raw_uid|dt|raw_info|
|--|--|--|
|12852|2020-01-13|1700 - 1715 de0010deg    633|
|12853|2020-01-13|1700 - 1715 de0010der    62|

2) There is also a trigger function named [`rescu.insert_rescu_volumes()`](create-trigger-function-populate_volumes_15min.sql) which processes the newly added data from `rescu.raw_15min` and inserts the processed data into the table `rescu.volumes_15min`. All information from `raw_info` in the raw table is then processed into the 3 columns which are `detector_id`, `datetime_bin` and `volume_15min` whereas `aretrycode` is taken from the table `rescu.detector_inventory` by matching them using `detector_id`. The table `rescu.volumes_15min` has the following information

|volume_uid|detector_id|datetime_bin|volume_15min|arterycode|
|--|--|--|--|--|
|9333459|DE0010DEG|2020-01-13 17:00:00|	749|	23984|
|9333460|DE0010DER|2020-01-13 17:00:00|	80|	3272|

### `check_rescu.py`

Since the terminal server does not have an internet connection, we will not get notified if the process fails. Therefore, we created an Airflow task to do that job for us. There is a dag named [`check_rescu`](/dags/check_rescu.py) which runs at 6am everyday that checks the number of rows inserted for the day before in both tables `rescu.raw_15min` and `rescu.volumes_15min`. If the number of rows is 0 OR if the number of rows from the raw table is less than that in the volumes table OR if the total number of rows from the volumes table is less than 7000 (the average number of rows per day is about 20,000), a Slack notification will be sent to notify the team. The line that does exactly that is shown below.
```python
if raw_num == 0 or raw_num < volume_num or volume_num < 7000:
  raise Exception ('There is a PROBLEM here. There is no raw data OR raw_data is less than volume_15min OR volumes_15min is less than 7000 which is way too low')
```

When the Slack message is sent, we can run the following check to find out what exactly is wrong with the data pipeline. The Airflow dag only shows us the number of rows in the raw and volumes tables but the reason of failing may still be unclear. Therefore, this query can be used to have a better picture on what is happening with that day of data.

```sql
SELECT 
TRIM(SUBSTRING(raw_info, 15, 12)) AS detector_id,
dt + LEFT(raw_info,6)::time AS datetime_bin,
nullif(TRIM(SUBSTRING(raw_info, 27, 10)),'')::int AS volume_15min
FROM rescu.raw_15min 
WHERE dt = '2020-09-03'::date --change to the date that you would like to investigate
AND nullif(TRIM(SUBSTRING(raw_info, 27, 10)),'')::int < 0
```

If the column `volume_15min` is `-1`, that means that there is something wrong with the data from the source end and we have to notify the person in charge as this is not something that we can fix. 
