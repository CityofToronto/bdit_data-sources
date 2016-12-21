
# Traffic Volumes
Turning Movement, Volume, and Occupancy counts from the FLOW database. Data tables currently stored in the `traffic` schema. 

## Loading Data
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

# Data Documentation
The following is an overview of tables relevant to traffic volume counts housed in FLOW (a database maintained by the City of Toronto's Traffic Management Centre) and that have been migrated to the Big Data Innovation Team's own PostgreSQL database. The relationships between the relevant tables are illustrated below. 

!['flow_tables_relationship'](img/flow_tables_relationship.png)

The database is structured around three types of tables: Automatic Traffic Recorder (ATR) counts, manual Turning Movement Counts (TMC), and other reference tables that provide additional spatial or temporal information.

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
