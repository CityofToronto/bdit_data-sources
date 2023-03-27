# BDIT Data Sources <!-- omit in toc -->

This is a master repo for all of the data sources that we use. Each folder is for a different data source and contains an explanation of what the data source is, how it can be used, a sample of the data, and scripts to import the data into the PostgreSQL database.

## Table of Contents <!-- omit in toc -->

- [Open Data Releases](#open-data-releases)
- [INRIX](#inrix)
- [BlipTrack Bluetooth Detectors](#bliptrack-bluetooth-detectors)
- [Text Description to Centreline Geometry Automation](#text-description-to-centreline-geometry-automation)
- [Volume Data](#volume-data)
	- [Miovision - Multi-modal Permanent Video Counters](#miovision---multi-modal-permanent-video-counters)
	- [RESCU - Loop Detectors](#rescu---loop-detectors)
	- [Turning Movement Counts (TMC)](#turning-movement-counts-tmc)
		- [Data Elements](#data-elements-4)
		- [Notes](#notes-2)
	- [Permanent Count Stations and Automated Traffic Recorder (ATR)](#permanent-count-stations-and-automated-traffic-recorder-atr)
		- [Data Elements](#data-elements-5)
		- [Notes](#notes-3)
- [Vehicle Detector Station (VDS)](#vehicle-detector-station-vds)
	- [Data Elements](#data-elements-6)
	- [Notes](#notes-4)
- [Incidents](#incidents)
	- [Data Elements](#data-elements-7)
	- [Notes](#notes-5)
- [Road Disruption Activity (RoDARS)](#road-disruption-activity-rodars)
	- [Data Elements](#data-elements-8)
	- [Notes](#notes-6)
- [Assets](#Assets)
  - [Traffic Signals](#Traffic-Signals)
- [CRASH - Motor Vehicle Accident Report](#crash---motor-vehicle-accident-report)
	- [Data Elements](#data-elements-9)
	- [Notes](#notes-7)
- [Vision Zero - Google Sheets API](#vision-zero---google-sheets-api)
	- [Data Elements](#data-elements-10)
- [`wys`: Watch Your Speed Signs](#wys-watch-your-speed-signs)

## Open Data Releases

- [Travel Times - Bluetooth](https://open.toronto.ca/dataset/travel-times-bluetooth/) contains data for all the bluetooth segments collected by the city. The travel times are 5 minute average travel times. The real-time feed is currently not operational. See [the Bluetooth README](bluetooth#8-open-data-releases) for more info.
- [Watch Your Speed Signs](#wys-watch-your-speed-signs) give feedback to drivers to encourage them to slow down, they also record speed of vehicles passing by the sign. Semi-aggregated and monthly summary data are available for the two programs (Stationary School Safety Zone signs and Mobile Signs) and are updated monthly.

For the [King St. Transit Pilot](toronto.ca/kingstreetpilot), the team has released the following datasets, which are typically a subset of larger datasets specific to the pilot:

- [King St. Transit Pilot - Detailed Bluetooth Travel Time](https://open.toronto.ca/dataset/king-st-transit-pilot-detailed-bluetooth-travel-time/) contains travel times collected during the King Street Pilot in the same format as the above data set. Data is collected on segments found in the [King St. Transit Pilot – Bluetooth Travel Time Segments](https://open.toronto.ca/dataset/king-st-transit-pilot-bluetooth-travel-time-segments/) map layer. See [the Bluetooth README](bluetooth#8-open-data-releases) for more info.
- [King St. Transit Pilot – Bluetooth Travel Time Summary](https://open.toronto.ca/dataset/king-st-transit-pilot-bluetooth-travel-time-summary/) contains monthly averages of corridor-level travel times by time periods. See [the Bluetooth README](bluetooth#8-open-data-releases) for more info.
- [King St. Transit Pilot - 2015 King Street Traffic Counts](https://open.toronto.ca/dataset/king-st-transit-pilot-2015-king-street-traffic-counts/) contains 15 minute aggregated ATR data collected during 2015 of various locations on King Street. See the [Volumes Open Data King Street Pilot](volumes#king-street-pilot) section for more info.
- [King St. Transit Pilot – Detailed Traffic & Pedestrian Volumes](https://open.toronto.ca/dataset/king-st-transit-pilot-detailed-traffic-pedestrian-volumes/) contains 15 minute aggregated TMC data collected from Miovision cameras during the King Street Pilot. The counts occurred at 31-32 locations at or around the King Street Pilot Area. See the [Miovision Open Data](miovision#open-data) section for more info.
- [King St. Transit Pilot - Traffic & Pedestrian Volumes Summary](https://open.toronto.ca/dataset/king-st-transit-pilot-traffic-pedestrian-volumes-summary/) is a monthly summary of the above data, only including peak period and east-west data. The data in this dataset goes into the [King Street Pilot Dashboard](https://www.toronto.ca/city-government/planning-development/planning-studies-initiatives/king-street-pilot/data-reports-background-materials/). See the [Miovision Open Data](miovision#open-data) section for more info.

## INRIX
Data collected from a variety of traffic probes from 2007 to 2016 for major streets and arterials.

## BlipTrack Bluetooth Detectors
The City collects traffic data from strategically placed sensors at intersections and along highways. These detect Bluetooth MAC addresses of vehicles as they drive by, which are immediately anonymized. When a MAC address is detected at two sensors, the travel time between the two sensors is calculated.

## Text Description to Centreline Geometry Automation

[`gis/text_to_centreline/`](gis/text_to_centreline) contains sql used to transform text description of street (in bylaws) into centreline geometries. See the [README](gis/text_to_centreline) for details on how to use.

## Volume Data

`volumes/` contains code and documentation on our many volume datasources:

- [`miovision`](#miovision---multi-modal-permanent-video-counters): Multi-modal permanent turning movement counts
- [`rescu`](#rescu---loop-detectors): ATR data from loop detectors
- [FLOW Data](volumes/#flow-data): A database of short-term ATR and TMCs

### Miovision - Multi-modal Permanent Video Counters

Miovision currently provides volume counts gathered by cameras installed at specific intersections. There are 32 intersections in total. Miovision then processes the video footage and provides volume counts in aggregated 1 minute bins. Data stored in 1min bin (TMC) is available in `miovision_api.volumes` whereas data stored in 15min bin for TMC is available in `miovision_api.volumes_15min_tmc` and data stored in 15min for ATR is available in `miovision_api.volumes_15min`. 

### RESCU - Loop Detectors
Road Emergency Services Communication Unit (RESCU) data tracks traffic volume on expressways using loop detectors. 
More information can be found on the [city's website](https://www.toronto.ca/services-payments/streets-parking-transportation/road-restrictions-closures/rescu-traffic-cameras/) 
or [here](https://en.wikipedia.org/wiki/Road_Emergency_Services_Communications_Unit).

### Turning Movement Counts (TMC)

### Automated Traffic Recorder (ATR) and Permanent Count Stations

## Vehicle Detector Station (VDS)

### Data Elements

* Location Identifier (SLSN Link ID)
* Count Type
* Roadway Names
* Lane Number
* 15 min aggregated interval times
* 15 min aggregated volume, occupancy, and speed

### Notes

* Raw 20sec interval VDS data is available on the processing server, not loaded into FLOW
* VDS device health/communication statuses are not recorded.
* Asset information managed in Excel spreadsheets
* Automated daily import but no real-time integration
* Strictly conforms to FLOW LOADER data file structure
* Quality control activities:  
  1. data gap verification
  2. partial data records flagged for manual verification/correction

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
Data available in `city.restrictions`

### Data Elements

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
	- Motorist/passenger/pedestrian/cyclist
	- Person age
	- Gender
	- Injuries and Fatalities

### Notes
* No real-time data integration
* Manual data integration with TPS and CRC via XML file exchange (not reliable or consistent)


## Assets
The `assets` directory stores [airflow](https://github.com/CityofToronto/bdit_team_wiki/blob/master/install_instructions/airflow.md) processes related to various assets that we help manage, such as datasets related to Vision Zero.  Below are the assets that we have automated so far.  

### Traffic Signals

A number of different features of traffic signals (Leading Pedestrian Intervals, Audible Pedestrian Signals, Pedestrian Crossovers, Traffic Signals) are periodically pulled from [OpenData](https://open.toronto.ca/dataset/traffic-signals-tabular/) . These indicators are used to populate the [Vision Zero Map](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-measures-and-mapping/) and [Dashboard](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/vision-zero-dashboard/). We have developed a process using Airflow to automatically connect to the database, extract the data needed, and store to our RDS Postgres database. See the README file in `assets/traffic_signals` for details about the source datasets and how they are combined into a final table made up of the following data elements.   

#### Data Elements
Field Name|Description|Type
----------|-----------|----
asset_type|type of indicator|text
px|?? | integer
main_street|name of main street|text
midblock_route|location details e.g. "26m SOUTH OF" |text
side1_street|name of intersecting street |text
side2_street|name of intersecting street if it e.g. changes after intersection |text
latitude|latitude|numeric
longitude|longitude|numeric
activation_date|date installed |date
details|currently NULL |text  

#### Notes  
For the final layer `vz_safety_programs.points_traffic_signals`, the `asset_type` column is populated with text `Traffic Signals`, and the other columns are renamed:  

```
'Traffic Signals'::text AS asset_type,
   a.id::integer AS px,
   a.streetname AS main_street,
   a.midblockroute AS midblock_route,
   a.side1routef AS side1_street,
   a.side2route AS side2_street,
   b.latitude,
   b.longitude,
   b.activation_date AS activation_date,
   NULL::text AS details
```

### Red Light Cameras

Red Light Camera data are obtained from Open Data and are also indicators that are displayed on the [Vision Zero Map](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-measures-and-mapping/) and [Dashboard](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/vision-zero-dashboard/). We have developed a process using Airflow to automatically connect to [Open Data](https://open.toronto.ca/dataset/red-light-cameras/) and store the data to our RDS Postgres database. See the README file in `assets/rlc` for details about this process.  The final table is made up of the following elements:  


#### Data Elements
Field Name|Description|Type
----------|-----------|----
rlc|ID number of camera |integer
tcs|?|integer
loc|name of intersection|text
additional_info| notes |text
main|name of main street in intersection|text
side1|name of intersecting street|text
side2|name of intersecting street if it e.g. changes after intersection |text
mid_block|currently all NULL|text
privateAccess|name of private access street|text
latitude|latitude|numeric
longitude|longitude|numeric
x|?|numeric
y|?|numeric
district|name of district|text
ward1|?|text
ward2|?|text
ward3|?|text
ward4|?|text
policeDivision1|?|text
policeDivision2|?|text
policeDivision3	|?|text
date_installed|date installed|date  

## Vision Zero - Google Sheets API

This dataset comes from Google Sheets tracking progress on implementation of safety improvements in school zones. \
Data Available in `vz_safety_programs_staging.school_safety_zone_2018_raw` and `vz_safety_programs_staging.school_safety_zone_2019_raw`

### Data Elements

Field Name|Description|Type
----------|-----------|----
school_name|name of school|text
address|address of school|text
work_order_fb|work order of flashing beacon|text
work_order_wyss|work order of watch your speed sign|text
locations_zone|coordinate of school|text
final_sign_installation|final sign installation date|text
locations_fb|location of flashing beacon|text
locations_wyss|location of watch your speed sign|text

## Watch Your Speed signs

The city has installed [Watch Your Speed signs](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-initiatives/initiatives/watch-your-speed-program/) that display the speed a vehicle is travelling at and flashes if the vehicle is travelling over the speed limit. Installation of the sign was done as part of 3 programs: the normal watch your speed sign program, mobile watch your speed which has signs mounted on trailers that move to a different location every few weeks, and school watch your speed which has signs installed at high priority schools. As part of the [Vision Zero Road Safety Plan](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/), these signs aim to reduce speeding.