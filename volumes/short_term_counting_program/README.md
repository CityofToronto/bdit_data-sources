# Short Term Traffic Volumes <!-- omit in toc -->

Short-term Traffic volume data (traffic counts and turning movements) from the FLOW database and other data sources.

## Table of Contents <!-- omit in toc -->

- [FLOW Data](#flow-data)
	- [1. Loading Data](#1-loading-data)
	- [2. Schema Overview](#2-schema-overview)
	- [3. Traffic Count Types](#3-traffic-count-types)
		- [Turning Movement Counts (TMCs)](#turning-movement-counts-tmcs)
			- [Data Elements](#data-elements)
			- [Notes](#notes)
		- [Automated Traffic Recorders (ATRs)](#automated-traffic-recorders-atrs)
			- [Data Elements](#data-elements-1)
			- [Notes](#notes-1)
	- [4. Relevant Tables](#4-relevant-tables)
		- [arterydata](#arterydata)
			- [Content](#content)
			- [Table Structure](#table-structure)
		- [det](#det)
			- [Content](#content-1)
			- [Table Structure](#table-structure-1)
		- [countinfomics](#countinfomics)
			- [Content](#content-2)
			- [Table Structure](#table-structure-2)
		- [cnt_det](#cnt_det)
			- [Content](#content-3)
			- [Table Stucture](#table-stucture)
		- [countinfo](#countinfo)
			- [Content](#content-4)
			- [Table Structure](#table-structure-3)
		- [category](#category)
			- [Content](#content-5)
			- [Table Structure](#table-structure-4)
	- [5. Useful Views](#5-useful-views)
- [Cycling Seasonality Adjustment](#cycling-seasonality-adjustment)

## FLOW Data

### 1. Loading Data

**Here is your quick July 2022 loading data update:**
- Data are loaded into the TRAFFIC_NEW schema on BigData every night
- For nine tables, data are upserted from TRAFFIC_NEW into traffic. The nine tables are:
    - arc_link
    - arterydata
    - category
    - cnt_det
    - cnt_spd
    - countinfo
    - countinfomics
    - det
    - nodes
- We audit some of these tables (in `traffic.logged_actions`) but not all of them because some of these tables are huge! With huge changes! Here are the tables we're auditing:
    - arc_link
    - arterydata
    - category
    - countinfo
    - countinfomics
    - nodes

### 2. Schema Overview
The following is an overview of tables relevant to traffic volume counts housed in FLOW, a database maintained by the Data Collection team within Transportation Services' Data & Analytics Unit. The relationships between the relevant tables are illustrated below.

!['flow_tables_relationship'](../img/flow_tables_relationship.png)


The database is structured around three types of tables: Turning Movement Counts (TMC), Automatic Traffic Recorder (ATR) counts, and other reference tables that provide additional spatial or temporal information.

Table Name|Description
----------|-----------
[arterydata](#arterydata)|Reference table for Artery Codes (internal reference for intersections and segments)
[det](#det)|Individual Turning Movement Count (TMC) observations
[countinfomics](#countinfomics)|Intermediate table linking TMC observations to Artery Codes
[cnt_det](#cnt_det)|Automatic Traffic Recorder (ATR) observations
[countinfo](#countinfo)|Intermediate table linking ATR observations to Artery Codes
[category](#category)|Reference table for Category ID (i.e traffic count type)

### 3. Traffic Count Types

#### Turning Movement Counts (TMCs)

##### Data Elements
* Location Identifier (SLSN *Node* ID)
* 15 min aggregated interval time
* 15 min aggregated volume per movement (turning and approach) by:
	- vehicle types
	- cyclists and pedestrian counts are approach only
	
##### Notes
* No regular data load schedule. 
* Data files collected by 2-3 staff members.
* Manually geo-reference volume data to an SLSN node during data import process.
* Data is manually imported into FLOW.
* Counts are conducted on Tuesdays, Wednesdays, and/or Thursdays during school season (September - June) for 1 to 3 consecutive days.
* Strictly conforms to FLOW LOADER data file structure.
* If collected data varies more than defined historical value threshold by 10%, the collected data will not be loaded.
* Volumes are available at both signalized and non-signalized intersections
* Each count station is given a unique identifier to avoid duplicate records.
* Data will not be collected under irregular traffic conditions (construction, closure, etc), but it maybe skewed by unplanned incidents.

#### Automated Traffic Recorders (ATRs)

##### Data Elements
* Location Identifier (SLSN *Link* ID)
* Direction
* 15 min aggregated interval time
* 15 min volume
	- typically aggregated by direction, although data may be available by lane

##### Notes
* The counts represent roadway and direction(s), not on a lane-by-lane level
* No regular data load schedule
* Manually geo-reference volume data to an SLSN node during data import process
* Strictly conforms to FLOW LOADER data file structure
* Typical ATR counts 24h * 3 days at location in either 1 or both directions
* Each PCS/ATR is given a unique identifier to avoid duplicate records

### 4. Relevant Tables

#### arterydata

##### Content

This table contains the location information of each volume count. 

##### Table Structure

Field Name|Type|Description
----------|----|-----------
arterycode|bigint|ID number referred to by [countinfomics](#countinfomics) and [countinfo](#countinfo)
street1|text|first street name
street2|text|second street name
location|text|full description of count location
apprdir|text|direction of the approach referred to by this arterycode
sideofint|text|the side of the intersection that the arterycode refers to
linkid|text|in the format of 8digits @ 8digits, with each 8 digits referring to a node

### det 
#### Content 
This table contains individual data entries for turning movement counts. For a long (vs wide) version of this table, see the matview `traffic.tmc_miovision_long_format`.

#### Table Structure
Field Name|Type|Description
----------|----|-----------
ID|Autonumber|Autonumber function
COUNT_INFO_ID|number|ID number linked to [countinfomics](#1. countinfomics) table containing higher-level information
COUNT_TIME|Date/Time|Effective time of counts (**time displayed is the end time period**)
N_CARS_R|number|S/B cars turning right
N_CARS_T|number|S/B cars going through
N_CARS_L|number|S/B cars turning left
S_CARS_R|number|N/B cars turning right
S_CARS_T|number|N/B cars going through
S_CARS_L|number|N/B cars turning left
E_CARS_R|number|W/B cars turning right
E_CARS_T|number|W/B cars going through
E_CARS_L|number|W/B cars turning left
W_CARS_R|number|E/B cars turning right
W_CARS_T|number|E/B cars going through
W_CARS_L|number|E/B cars turning left
N_TRUCK_R|number|S/B trucks turning right
N_TRUCK_T|number|S/B trucks going through
N_TRUCK_L|number|S/B trucks turning left
S_TRUCK_R|number|N/B trucks turning right
S_TRUCK_T|number|N/B trucks going through
S_TRUCK_L|number|N/B trucks turning left
E_TRUCK_R|number|W/B trucks turning right
E_TRUCK_T|number|W/B trucks going through
E_TRUCK_L|number|W/B trucks turning left
W_TRUCK_R|number|E/B trucks turning right
W_TRUCK_T|number|E/B trucks going through
W_TRUCK_L|number|E/B trucks turning left
N_BUS_R|number|S/B buses turning right
N_BUS_T|number|S/B buses going through
N_BUS_L|number|S/B buses turning left
S_BUS_R|number|N/B buses turning right
S_BUS_T|number|N/B buses going through
S_BUS_L|number|N/B buses turning left
E_BUS_R|number|W/B buses turning right
E_BUS_T|number|W/B buses going through
E_BUS_L|number|W/B buses turning left
W_BUS_R|number|E/B buses turning right
W_BUS_T|number|E/B buses going through
W_BUS_L|number|E/B buses turning left
N_PEDS|number|North side pedestrians
S_PEDS|number|South side pedestrians
E_PEDS|number|East side pedestrians
W_PEDS|number|West side pedestrians
N_BIKE|number|S/B bicycles from the north side
S_BIKE|number|N/B bicylcles from the south side
E_BIKE|number|W/B bicycles from the east side
W_BIKE|number|E/B bicycles from the west side
N_OTHER|number|North side  - optional field
S_OTHER|number|South side - optional field
E_OTHER|number|East side - optional field
W_OTHER|number|West side - optional field

#### countinfomics

##### Content

This table contains the location, date, and source for each count_info_id. This table contains turning movement counts information exclusively.

##### Table Structure

Field Name|Type|Description
----------|----|-----------
count_info_id|bigint|ID number linked to [det](#det) table containing detailed count entries
arterycode|bigint|ID number linked to [arterydata](#arterydata) table containing information for the count location
count_date|date|date on which the count was conducted
day_no|bigint|day of the week
category_id|int|ID number linked to [category](#category) table containing the source of the count

#### cnt_det

##### Content

This table contains individual data entries from all sources other than turning movement counts.

##### Table Stucture

Field Name|Type|Description
----------|----|-----------
count_info_id|bigint|ID number linked to [countinfo](#countinfo) table containing higher-level information
count|bigint|vehicle count
timecount|Date/Time|Effective time of counts (**time displayed is the end time period**) (**except for ATRs, where time is the start of the count**)
speed_class|int|Speed class codes indicating speed bins associated with the 'prj_volume.speed_classes' table. speed_class=0 refers to non-speed counts

#### countinfo

##### Content

Similar to [countinfomics](#countinfomics), this table contains the location, date, and source for each count_info_id from all sources other than turning movement counts.

##### Table Structure

See [countinfomics](#countinfomics)

#### category

##### Content

This is a reference table referencing the data source of each entry.

##### Table Structure

Field Name|Type|Description
----------|----|-----------
category_id|int|ID number referred to by [countinfomics](#countinfomics) and [countinfo](#countinfo)
category_name|text|name of the data source

### 5. Useful Views

- `traffic.tmc_miovision_long_format` - Takes the wide TMC table `traffic.det` and transforms it into a long format designed to be integrated with miovision-derived TMCs as in `miovision_api.volumes_15min_mvt`. 

- `traffic.artery_locations_px` -  A lookup view between artery codes and px numbers (intersections), created using `regexp_matches`. 

- `traffic.artery_traffic_signals` - A lookup view between artery codes and px numbers that have traffic signals. 

- `traffic.artery_objectid_pavement_asset` - A lookup view between artery codes and objectid. Used, for example, to link an arterycode to pavement asset information in vz_analysis.gcc_pavement_asset. This view uses the intermediate table `gis_shared_streets.centreline_pavement_180430` which was last updated three years ago and it will be updated via issue [Update pavement assets #620](https://github.com/CityofToronto/bdit_data-sources/issues/620).

## Cycling Seasonality Adjustment

A model was developed to adjust cycling counts for before after evaluations of new infrastructure based on sparse counts. It can be found in the [`cycling_seasonality`](cycling_seasonality/) folder
