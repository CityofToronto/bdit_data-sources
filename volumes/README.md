
# Traffic Volumes
Turning Movement, Volume, and Occupancy counts from the FLOW database. Data tables currently stored in the `traffic` schema. 

## 1. Loading Data
The data in the schema comes from an image of FLOW Oracle database, which was reconstituted with a free version of [Oracle Database](http://www.oracle.com/technetwork/database/database-technologies/express-edition/downloads/index.html), [Oracle SQL Developer](http://www.oracle.com/technetwork/developer-tools/sql-developer/downloads/index-098778.html) to view the table structures and data. `impdp` was used to import first the full schema, and then select tables of data to import into a local Oracle DB.

In a SQL window, create the `TRAFFIC` tablespace for import.
```sql
CREATE TABLESPACE TRAFFIC DATAFILE 'traffic.dbf' SIZE 10600M ONLINE;
```
And then to import tables
```shell
impdp system/pw tables=(traffic.det, traffic.cal) directory=flow_data dumpfile=flow_traffic_data.dmp logfile=flow_traffic_data.log table_exists_action=truncate
```
For one really large table (larger than the 11GB max database size for Oracle Express) it was necessary to [filter rows in the table](https://dba.stackexchange.com/questions/152326/how-can-i-restore-a-10-91g-table-in-oracle-express).

Once the data is imported into Oracle, it was dumped to csv + sql file in SQL Developer. The `CREATE TABLE` sql file was converted to PostgreSQL-friendly code with [SQLines](http://www.sqlines.com/home).

## 2. Data Documentation
The following is an overview of tables relevant to traffic volume counts housed in FLOW (a database maintained by the City of Toronto's Traffic Management Centre) and that have been migrated to the Big Data Innovation Team's own PostgreSQL database. The relationships between the relevant tables are illustrated below. 

!['flow_tables_relationship'](img/flow_tables_relationship.png)

The database is structured around three types of tables: Automatic Traffic Recorder (ATR) counts, manual Turning Movement Counts (TMC), and other reference tables that provide additional spatial or temporal information.

### countinfomics
#### Content
This table contains the location, date, and source for each count_info_id. This table contains turning movement counts information exclusively.

#### Table Structure
Field Name|Type|Description
----------|----|-----------
count_info_id|bigint|ID number linked to [det](#det) table containing detailed count entries
arterycode|bigint|ID number linked to [arterydata](#arterydata) table containing information for the count location
count_date|date|date on which the count was conducted
day_no|bigint|day of the week
category_id|int|ID number linked to [category](#category) table containing the source of the count


### det 
#### Content 
This table contains individual data entries for turning movement counts

#### Table Structure
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

### countinfo
#### Content
Similar to [countinformics](#countinfomics), this table contains the location, date, and source for each count_info_id from all sources other than turning movement counts.

#### Table Structure
See [countinformics](#countinfomics)


### cnt_det
#### Content
This table contains individual data entries from all sources other than turning movement counts.

#### Table Stucture
Field Name|Type|Description
----------|----|-----------
count_info_id|bigint|ID number linked to [countinfo](#countinfo) table containing higher-level information
count|bigint|vehicle count
timecount|Date/Time|Effective time of counts (time displayed is the end time period)


### arterydata
#### Content
This table contains the location information of each volume count. 

#### Table Structure
Field Name|Type|Description
----------|----|-----------
arterycode|bigint|ID number referred to by [countinformics](#countinfomics) and [countinfo](#countinfo)
street1|text|first street name
street2|text|second street name
location|text|full description of count location
apprdir|text|direction of the approach referred to by this arterycode
sideofint|text|the side of the intersection that the arterycode refers to
linkid|text|in the format of 8digits @ 8digits, with each 8 digits referring to a node

### category
#### Content
This is a reference table referencing the data source of each entry.

#### Table Structure
Field Name|Type|Description
----------|----|-----------
category_id|int|ID number referred to by [countinformics](#countinfomics) and [countinfo](#countinfo)
category_name|text|name of the data source


## Turning Movement Counts

### Data Elements
* Location Identifier (SLSN Node ID)
* CountType
* Count interval start and end date and times
* AM Peak, PM peak, and off-peak 7:30-9:30, 10:00-12:00,13:00-15:00,16:00-18:00
* Roadway 1 and 2 names (intersectoin)
* 15 min aggregated interval time
* 15 min aggregated volume per movement (turning and approach) by:
	- vehicle types
	- cyclists and pedestrian counts are approach only
	
### Notes
* No regular data load schedule. 
* Data files collected by 2-3 staff members.
* Manually geo-reference volume data to an SLSN node during data import process
* Data is manually updated into FLOW.
* Counts are conducted on Tuesdays, Wednesdays, and/or Thursdays during school season (September - June) for 1 to 3 consecutive days
* Strictly conforms to FLOW LOADER data file structure
* If numbers collected varies more than defined historical value threshold by 10%, the count will not be loaded.
* Volume available at both signalized and non-signalized intersections
* Each count station is given a unique identifier to avoid duplicate records
* Data will not be collected under irregular traffic conditions(construction, closure, etc), but it maybe skewed by unplanned incidents.

## Permanent Count Stations and Automated Traffic Recorder

### Data Elements
* Location Identifier (SLSN *Link* ID)
* Count Type
* Count interval start and end date and times
* Roadway Names
* Location Description
* Direction
* Number of Lanes
* Median and Type
* Comments
* 15 min aggregated interval time
* 15 min volume

### Notes
* The counts represent roadway and direction(s), not on a lane-by-lane level
* No regular data load schedule
* Manually geo-reference volume data to an SLSN node during data import process
* Strictly conforms to FLOW LOADER data file structure
* Typical ATR counts 24h * 3 days at location in either 1 or both directions
* Each PCS/ATR is given a unique identifier to avoid duplicate records
