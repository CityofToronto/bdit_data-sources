# FLOW TABLES
This document provides an overview of the tables in FLOW (The City's traffic count database) that have being extracted to feed into the centralized Postgres database, and sepcifically acting as the raw data input to build city-wide volume profiles for all City roads.
The relationships between the relevant tables are shown below. The database is structured aroudn three types of tables: Automatic Traffic Recorder(ATR) counts, manual Turning Movement Counts (TMC) and couunt information tables that detail the geography and count request information.

!['flow_tables_relationship'](img/flow_tables_relationship.png)

More details on each individual table are presented below. Note there are additional fields in the raw tables that are not listed below as they are not relevant to the study scope.

## countinfomics
### Content
This table contains the location, date, and source for each count_info_id. This table contains turning movement counts information exclusively.

### Table Structure
Field Name|Type|Description
----------|----|-----------
count_info_id|bigint|ID number linked to [det](#det) table containing detailed count entries
arterycode|bigint|ID number linked to [arterydata](#arterydata) table containing information for the count location
count_date|date|date on which the count was conducted
day_no|bigint|day of the week
category_id|int|ID number linked to [category](#category) table containing the source of the count

## det 
### Content 
This table contains individual data entries for turning movement counts

### Table Structure
Field Name|Type|Description
----------|----|-----------
ID|Autonumber|Autonumber function
COUNT_INFO_ID|number|ID number linked to [countinfomics](#1. countinfomics) table containing higher-level information
COUNT_TIME|Date/Time|Effective time of counts (time displayed is the end time period)
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

## countinfo
### Content
Similar to [countinformics](#countinfomics), this table contains the location, date, and source for each count_info_id from all sources other than turning movement counts.

### Table Structure
See [countinformics](#countinfomics)

## cnt_det
### Content
This table contains individual data entries from all sources other than turning movement counts.

### Table Stucture
Field Name|Type|Description
----------|----|-----------
count_info_id|bigint|ID number linked to [countinfo](#countinfo) table containing higher-level information
count|bigint|vehicle count
timecount|Date/Time|Effective time of counts (time displayed is the end time period)

## arterydata
### Content
This table contains the location information of each volume count. 

### Table Structure
Field Name|Type|Description
----------|----|-----------
arterycode|bigint|ID number referred to by [countinformics](#countinfomics) and [countinfo](#countinfo)
street1|text|first street name
street2|text|second street name
location|text|full description of count location
apprdir|text|direction of the approach referred to by this arterycode
sideofint|text|the side of the intersection that the arterycode refers to
linkid|text|in the format of 8digits @ 8digits, with each 8 digits referring to a node

## category
### Content
This is a reference table referencing the data source of each entry.

### Table Structure
Field Name|Type|Description
----------|----|-----------
category_id|int|ID number referred to by [countinformics](#countinfomics) and [countinfo](#countinfo)
category_name|text|name of the data source
