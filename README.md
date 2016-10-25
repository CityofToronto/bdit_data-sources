# BDIT Data Sources

This is a master repo for all of the data sources that we use. Each folder is for a different data source and contains an explanation of what the data source is and how it can be used, a sample of the data, and scripts to import the data into the PostgreSQL database.

##INRIX

###Data Elements
Field Name|Description|Type
----------|-----------|----
RoadName/Number|Road names or numbers|string
tx|Date and time|datetime
tmc|TMC Link ID|string
spd|Link speed estimate|double
count|Sample Size Used|int
score|Quality Indicator|10/20/30

###Notes
* INRIX vehicles disproportionately include heavy vehicles. 
* There's additional sampling bias in that the heavy vehicles do not reflect the general travel patterns.
* Two sections of freeway(the southernmost sections of 427 and 404) have no available data. These approaches may be imssing due to the idiosyncratic geometries of TMCs near the major freeway-to-freeway interchanges.
* In any given 15 minute interval between 5am and 10pm, 88.7% of freeway links and 48% of arterial links have observations.

##BlipTrack Bluetooth Detectors

###Data Elements
####Historical Data
Field Name|Description|Type
----------|-----------|----
TimeStamp|timestamp|datetime
StartPointName|startpoint name of segment|string
EndPointName|endpoint name of segment |string
MinMeasuredTime|min waiting time of users completing the route from start to end in the timeframe timestamp-resolution to timestamp|int
MaxMeasuredTime|max waiting time of users completing the route from start to end in the timeframe timestamp-resolution to timestamp|int
AvgMeasuredTime|average waiting time of users completing the route from start to end in the timeframe timestamp-resolution to timestamp|int
MedianMeasuredTime|median waiting time of users completing the route from start to end in the timeframe timestamp-resolution to timestamp|int
SampleCount|the number of devices completing the route from start to end in the timeframe timestamp-resolution to timestamp|int

###Retrieval
* Interfaces for retrieving data
	1. Export from Bliptrack GUI at g4apps.bliptrack.net
	2. Display API: REST-based interface returns live data as JSON or xml in a HTTP(S) response
	3. WebService API: SOAP-based for programmatic access to the system.
* Functions that web service includes:
	- getAvailableDisplayIds() – returns a list of available Public Displays;
	- getDisplayInfo() – returns detailed information for a Public Display for a given display ID;
	- getPublicDisplayData() – used to get Public Display data (current result set) for a given display ID;
	- getDisplayData() – used to get Public Display data (current result set) for a given display ID (for displays with Restricted Access enabled);
	- getExportableAnalyses() – returns a list of analyses, each with all required information for export;
	- getExportableLiveAnalyses() – returns a list of live analyses, each with all required;
	- information for export;
	- getFilteredAnalyses() – returns a list of analyses matching a specified filter, each with all required information for export;
	- exportDwelltimeReport() – used to export Measured Time data;
	- exportLiveDwelltimeReport() – used to export Live Measured Time data;
	- exportKPIReport() – used to export KPI data;
	- exportQuarterlyReport() – used to export Quarterly KPI data;	
	- exportCounterReport() – used to export Counter Reports;
	- getCurrentDwellTime() – used to get current dwell time for a live analysis;
	- getCurrentDwellTimes() – used to get current dwell time for a list of live analyses in a single call;
	- exportPerUserData() – used to export individual dwell time measurements for an analysis; and
	- getCustomCurrentDwellTime() – used to get current dwell time for a live analysis with custom parameters.
	
##Turning Movement Counts

###Data Elements
* Location Identifier (SLSN Node ID)
* CountType
* Count interval start and end date and times
* AM Peak, PM peak, and off-peak 7:30-9:30, 10:00-12:00,13:00-15:00,16:00-18:00
* Roadway 1 and 2 names (intersectoin)
* 15 min aggregated interval time
* 15 min aggregated volume per movement (turning and approach) by:
	** vehicle types
	** cyclists and pedestrian counts are approach only
	
###Notes
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

##Permanent Count Stations and Automated Traffic Recorder

###Data Elements
* Location Identifier(SLSN *Link* (Node?) ID)
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

###Notes
* The counts represent roadway and direction(s), not on a lane-by-lane level
* No regular data load schedule
* Manually geo-reference volume data to an SLSN node during data import process
* Strictly conforms to FLOW LOADER data file structure
* Typical ATR counts 24h * 3 days at location in either 1 or both directions
* Each PCS/ATR is given a unique identifier to avoid duplicate records

## Vehicle Detector Station (VDS)

###Data Elements
* Location Identifier (SLSN Link ID)
* Count Type
* Roadway Names
* Lane Number 
* 15 min aggregated interval times
* 15 min aggregated volume, occupancy, and speed

###Notes
* Raw 20sec interval VDS data is available on the processing server, not loaded into FLOW
* VDS device health/communication statuses are not recorded.
* Asset information managed in Excel spreadsheets
* Automated daily import but no real-time integration
* Strictly conforms to FLOW LOADER data file structure
* Quality control activities: 1. data gap verification 2. partial data records flagged for manual verification/correction

## Incidents

### Data Elements
* Unique system (ROdb) identifier
* Location
* DTO district
* Incident start and end times
* Incident description free form
* Incident status and timestamps
* Police activities and timestamps
* RESCU operator shift information

### Notes
* Manual data entry
* Location description from a dropdown list
* Manual location selection on a map based on location description

## Road Disruption Activity (RoDARS)
Data available in big_data.city.restrictions
###Data Elements
Field Name|Description|Type
----------|-----------|----
id|Unique system identifier|string
description|project description freeform (direction and number of lanes affected and reason)|string
name|location description|string
road|road of disruption|string
atroad|road at cross if disruption zone is an intersection|string
fromroad|start crossroad if disruption zone is a segment|string
toroad|end crossroad if disruption zone is a segment|stirng
latitude/longitude|geo-information (not always occupied)|double
district|district of location|string(from dropdown list)
roadclass|road types|string(from dropdown list)
expired|event status|0:ongoing;1:expired
starttime|start time(may not be accurate)|timestamp
endtime|end time(may not be accurate)|timestamp
workperiod|Daily/Continuous/Weekdays/Weekends(not always occupied)|string
contractor|contractor name(not always occupied)|string(from dropdown list)
workeventtype|work event types(not always occupied)|string(from dropdown list)

### Notes
* Information is collected via applicant submission of RoDARS notification form
* Data entry into ROdb via dropdown list of values
* No system integration with special events and filming departmental systems
* Crucial elements of information are in free-form such as lane blockage/closure
* Roadway names from a dropdown list that conforms to SLSN

## CRASH - Motor Vehicle Accident Report
### Data Elements
* Unique MVAR identifier (externally assigned by Toronto Police Service)
* Accident Date & Time
* Type of collision
* Accident location: street name(s), distance offset, municipality, county, etc.
* Description of accident and diagram
* Involved persons:
	** Motorist/passenger/pedestrian/cyclist
	** Person age
	** Gender
	** Injuries and Fatalities
###Notes
* No real-time data integration
* Manual data integration with TPS and CRC via XML file exchange (not reliable or consistent)
