# BDIT Data Sources <!-- omit in toc -->

This is a master repo for all of the data sources that we use. Each folder is for a different data source and contains an explanation of what the data source is, how it can be used, a sample of the data, and scripts to import the data into the PostgreSQL database.

## Table of Contents <!-- omit in toc -->

- [Assets](#Assets)
  - [Red Light Cameras](#Red-light-cameras)
  - [Traffic Signals](#Traffic-Signals)
- [Bluetooth Detectors](#bluetooth-detectors)
- [Collisions](#collisions)
- [Incidents](#incidents)
- [INRIX](#inrix)
- [Street Centreline Geocoding](#street-centreline-geocoding)
- [Volume Data](#volume-data)
	- [Miovision - Multi-modal Permanent Video Counters](#miovision---multi-modal-permanent-video-counters)
	- [RESCU - Loop Detectors](#rescu---loop-detectors)
	- [Turning Movement Counts (TMC)](#turning-movement-counts-tmc)
	- [Permanent Count Stations and Automated Traffic Recorder (ATR)](#permanent-count-stations-and-automated-traffic-recorder-atr)
- [Vehicle Detector Station (VDS)](#vehicle-detector-station-vds)
	- [Data Elements](#data-elements-6)
	- [Notes](#notes-4)
- [School Safety Zones](#school-safety-zones)
- [Watch Your Speed Signs](#watch-your-speed-signs)

## Assets
[`assets/`](assets/)

The `assets` directory stores [airflow](https://github.com/CityofToronto/bdit_team_wiki/blob/master/install_instructions/airflow.md) processes related to various assets that we help manage, such as datasets related to Vision Zero.  Below are the assets that we have automated so far.  

### Red Light Cameras
[`assets/rlc/`](assets/rlc/)

Red Light Camera data are obtained from Open Data and are also indicators that are displayed on the [Vision Zero Map](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-measures-and-mapping/) and [Dashboard](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/vision-zero-dashboard/). We have developed a process using Airflow to automatically connect to [Open Data](https://open.toronto.ca/dataset/red-light-cameras/) and store the data to our RDS Postgres database. See the README file in [`assets/rlc`](assets/rlc/) for details about this process.

### Traffic Signals
[`assets/traffic_signals/`](assets/traffic_signals/)

A number of different features of traffic signals (Leading Pedestrian Intervals, Audible Pedestrian Signals, Pedestrian Crossovers, Traffic Signals) are periodically pulled from [OpenData](https://open.toronto.ca/dataset/traffic-signals-tabular/) . These indicators are used to populate the [Vision Zero Map](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-measures-and-mapping/) and [Dashboard](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/vision-zero-dashboard/). See the README file in [`assets/traffic_signals`](assets/traffic_signals/) for details about the source datasets and how they are combined into a final table made up of the following data elements.

## Bluetooth Detectors
[`bluetooth/`](bluetooth/)

The City collects traffic data from strategically placed sensors at intersections and along highways. These detect Bluetooth MAC addresses of vehicles as they drive by, which are immediately anonymized. When a MAC address is detected at two sensors, the travel time between the two sensors is calculated.

## Collisions

[`collisions/`](collisions/)

The collisions dataset consists of data on individuals involved in traffic collisions from approximately 1985 to the present day (though there are some historical collisions from even earlier included).

## Incidents
See [CityofToronto/bdit_incidents](https://github.com/CityofToronto/bdit_incidents)

## INRIX
[`inrix/`](inrix/)

Data collected from a variety of traffic probes from 2007 to 2016 for major streets and arterials.

## Street Centreline Geocoding
[`gis/text_to_centreline/`](gis/text_to_centreline/)

Contains SQL used to transform text description of street (in bylaws) into centreline geometries.

## Volume Data
[`volumes/`](volumes/)


### Miovision - Multi-modal Permanent Video Counters
[`volumes/miovision/`](volumes/miovision/)

Miovision currently provides volume counts gathered by cameras installed at specific intersections. There are 32 intersections in total. Miovision then processes the video footage and provides volume counts in aggregated 1 minute bins. Data stored in 1min bin (TMC) is available in `miovision_api.volumes` whereas data stored in 15min bin for TMC is available in `miovision_api.volumes_15min_tmc` and data stored in 15min for ATR is available in `miovision_api.volumes_15min`. 

### RESCU - Loop Detectors
[`volumes/rescu/`](volumes/rescu/)

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

## School Safety Zones
[`gis/school_safety_zones/`](gis/school_safety_zones/)

This dataset comes from Vision Zero which uses Google Sheets to track progress on the implementation of safety improvements in school zones.

## Watch Your Speed signs
[`wys/`](wys/)

The city has installed [Watch Your Speed signs](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-initiatives/initiatives/watch-your-speed-program/) that display the speed a vehicle is travelling at and flashes if the vehicle is travelling over the speed limit. Installation of the sign was done as part of 3 programs: the normal watch your speed sign program, mobile watch your speed which has signs mounted on trailers that move to a different location every few weeks, and school watch your speed which has signs installed at high priority schools. As part of the [Vision Zero Road Safety Plan](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/), these signs aim to reduce speeding.