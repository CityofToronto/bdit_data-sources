# BDIT Data Sources <!-- omit in toc -->

This is the primary repository for code and documentation for most the data sources the Data & Analytics Unit uses.

Each folder is for a different data source (or category of related data sources). They contain:
* an explanation of what the data source is,
* how it can be used, and
* the Python and SQL necessary for our Extract, Load, Transform, and Validate processes into our PostgreSQL database.

For those curious about what data we manage is released on OpenData, see the [Open Data Releases](#open-data-releases).

## Table of Contents <!-- omit in toc -->

- [Airflow DAGS](#airflow-dags)
- [Bluetooth Detectors](#bluetooth-detectors)
- [Collisions](#collisions)
- [Cycling App (inactive)](#cycling-app-inactive)
- [Events](#events)
- [GIS - Geographic Data](#gis---geographic-data)
  - [Assets](#assets)
    - [Red Light Cameras](#red-light-cameras)
    - [Traffic Signals](#traffic-signals)
  - [School Safety Zones](#school-safety-zones)
  - [Street Centreline Geocoding](#street-centreline-geocoding)
- [HERE Travel Time Data](#here-travel-time-data)
- [Incidents (inactive)](#incidents-inactive)
- [INRIX (inactive)](#inrix-inactive)
- [Parking (inactive)](#parking-inactive)
- [TTC (inactive)](#ttc-inactive)
- [Volume Data](#volume-data)
  - [Miovision - Multi-modal Permanent Video Counters](#miovision---multi-modal-permanent-video-counters)
  - [RESCU - Loop Detectors (inactive)](#rescu---loop-detectors-inactive)
  - [Short-term Counting Program](#short-term-counting-program)
  - [Vehicle Detector Station (VDS)](#vehicle-detector-station-vds)
- [Watch Your Speed signs](#watch-your-speed-signs)
- [Weather](#weather)
- [Open Data Releases](#open-data-releases)

## Airflow DAGS

[`dag/`](dag/)

This folder contains the DAG Python files for our Airflow orchestration that dictate the logic and schedule for data pipeline tasks.

## Bluetooth Detectors
[`bluetooth/`](bluetooth/)

The City collects traffic data from strategically placed sensors at intersections and along highways. These detect Bluetooth MAC addresses of vehicles as they drive by, which are immediately anonymized. When a MAC address is detected at two sensors, the travel time between the two sensors is calculated.

## Collisions

[`collisions/`](collisions/)

The collisions dataset consists of data on individuals involved in traffic collisions from approximately 1985 to the present day (though there are some historical collisions from even earlier included).

## Cycling App (inactive)

[`cycling_app/`](cycling_app/)

The Cycling App collected OD and trip data until 2016.

## Events

[`events/`](events/)

How does construction and special events impact traffic in the city?
- City road permitting data (RoDARs)
- (oudated) Special events from City's Open Data and TicketMaster

## GIS - Geographic Data

### Assets
[`assets/`](gis/assets/)

The `assets` directory stores [airflow](https://github.com/CityofToronto/bdit_team_wiki/blob/master/install_instructions/airflow.md) processes related to various assets that we help manage, such as datasets related to Vision Zero.  Below are the assets that we have automated so far.  

#### Red Light Cameras
[`assets/rlc/`](gis/assets/rlc/)

Red Light Camera data are obtained from Open Data and are also indicators that are displayed on the [Vision Zero Map](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-measures-and-mapping/) and [Dashboard](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/vision-zero-dashboard/). We have developed a process using Airflow to automatically connect to [Open Data](https://open.toronto.ca/dataset/red-light-cameras/) and store the data to our RDS Postgres database. See the README file in [`assets/rlc`](assets/rlc/) for details about this process.

#### Traffic Signals
[`assets/traffic_signals/`](gis/assets/traffic_signals/)

A number of different features of traffic signals (Leading Pedestrian Intervals, Audible Pedestrian Signals, Pedestrian Crossovers, Traffic Signals) are periodically pulled from [OpenData](https://open.toronto.ca/dataset/traffic-signals-tabular/) . These indicators are used to populate the [Vision Zero Map](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-measures-and-mapping/) and [Dashboard](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/vision-zero-dashboard/). See the README file in [`assets/traffic_signals`](assets/traffic_signals/) for details about the source datasets and how they are combined into a final table made up of the following data elements.

### School Safety Zones

[`gis/school_safety_zones/`](gis/school_safety_zones/)

This dataset comes from Vision Zero which uses Google Sheets to track progress on the implementation of safety improvements in school zones.


### Street Centreline Geocoding

[`gis/text_to_centreline/`](gis/text_to_centreline/)

Contains SQL used to transform text description of street (in bylaws) into centreline geometries.

## HERE Travel Time Data

[`here/`](here/)

Travel time data provided by HERE Technologies from a mix of vehicle probes. Daily extracts of 5-min aggregated speed data for each link in the city (where data are available).

## Incidents (inactive)

See [CityofToronto/bdit_incidents](https://github.com/CityofToronto/bdit_incidents)

## INRIX (inactive)

[`inrix/`](inrix/)

Data collected from a variety of traffic probes from 2007 to 2016 for major streets and arterials.

## Parking (inactive)
[`parking/`](parking/)

This contains R and SQL files for pulling parking lots and parking tickets from Open Data. They might be useful but haven't been documented or automated.

## TTC (inactive)
[`ttc/`](ttc/) 

This contains some valiant attempts at transforming CIS vehicle location data provided to us by the TTC on streetcar locations as well as an automated process for pulling in GTFS schedule data.

## Volume Data
[`volumes/`](volumes/)


### Miovision - Multi-modal Permanent Video Counters
[`volumes/miovision/`](volumes/miovision/)

Miovision currently provides volume counts gathered by cameras installed at specific intersections. There are 32 intersections in total. Miovision then processes the video footage and provides volume counts in aggregated 1 minute bins. Data stored in 1min bin (TMC) is available in `miovision_api.volumes` whereas data stored in 15min bin for TMC is available in `miovision_api.volumes_15min_tmc` and data stored in 15min for ATR is available in `miovision_api.volumes_15min`. 

### RESCU - Loop Detectors (inactive)
[`volumes/rescu/`](volumes/rescu/)

Deprecated. See [Vehicle Detector Station (VDS)](#vehicle-detector-station-vds). 

### Short-term Counting Program
[`volumes/short_term_counting_program/`](volumes/short_term_counting_program/)

Short-term traffic counts are conducted on an ad-hoc basis as the need arises, and may be done throughout the year both at intersections and mid-block. Much of this dataset is also available through the internal application MOVE and data go as far back as 1994. As of January 2025, The bulk of this data is now available to the public on the Open Data pages:
-  [Traffic Volumes - Midblock Vehicle Speed, Volume and Classification Counts](https://open.toronto.ca/dataset/traffic-volumes-midblock-vehicle-speed-volume-and-classification-counts/) 
- [Traffic Volumes - Multimodal Intersection Turning Movement Counts](https://open.toronto.ca/dataset/traffic-volumes-at-intersections-for-all-modes/)

### Vehicle Detector Station (VDS)
[`volumes/vds/`](volumes/vds/)

The city operates various permanent Vehicle Detector Stations (VDS), employing different technologies, including RESCU, intersection detectors, Blue City and Smartmicro. The most frequently used for D&A context is the RESCU network which tracks traffic volumes on Toronto expressways, about which more information can be found on the [city's website](https://www.toronto.ca/services-payments/streets-parking-transportation/road-restrictions-closures/rescu-traffic-cameras/) 
or [here](https://en.wikipedia.org/wiki/Road_Emergency_Services_Communications_Unit).


## Watch Your Speed signs
[`wys/`](wys/)

The city has installed [Watch Your Speed signs](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-initiatives/initiatives/watch-your-speed-program/) that display the speed a vehicle is travelling at and flashes if the vehicle is travelling over the speed limit. Installation of the sign was done as part of 2 programs: the mobile watch your speed which has signs mounted on existing poles, moved every few weeks, and school watch your speed which has signs installed at high priority schools. The signs also collect continuous speed data.

## Weather
[`weather/`](weather/)

Daily historical weather conditions and predictions from Environment Canada.

## Open Data Releases

- [Travel Times - Bluetooth](https://open.toronto.ca/dataset/travel-times-bluetooth/) contains data for all the bluetooth segments collected by the city. The travel times are 5 minute average travel times. The real-time feed is currently not operational. See [the Bluetooth README](bluetooth#8-open-data-releases) for more info.
- [Watch Your Speed Signs](#watch-your-speed-signs) give feedback to drivers to encourage them to slow down, they also record speed of vehicles passing by the sign. Semi-aggregated and monthly summary data are available for the two programs (Stationary School Safety Zone signs and Mobile Signs) and are updated monthly.  [see the WYS README for links to these datasets](wys/#open-data)

For the [King St. Transit Pilot](toronto.ca/kingstreetpilot), the team has released the following datasets, which are typically a subset of larger datasets specific to the pilot:

- [King St. Transit Pilot - Detailed Bluetooth Travel Time](https://open.toronto.ca/dataset/king-st-transit-pilot-detailed-bluetooth-travel-time/) contains travel times collected during the King Street Pilot in the same format as the above data set. Data is collected on segments found in the [King St. Transit Pilot – Bluetooth Travel Time Segments](https://open.toronto.ca/dataset/king-st-transit-pilot-bluetooth-travel-time-segments/) map layer. See [the Bluetooth README](bluetooth#8-open-data-releases) for more info.
- [King St. Transit Pilot – Bluetooth Travel Time Summary](https://open.toronto.ca/dataset/king-st-transit-pilot-bluetooth-travel-time-summary/) contains monthly averages of corridor-level travel times by time periods. See [the Bluetooth README](bluetooth#8-open-data-releases) for more info.
- [King St. Transit Pilot - 2015 King Street Traffic Counts](https://open.toronto.ca/dataset/king-st-transit-pilot-2015-king-street-traffic-counts/) contains 15 minute aggregated ATR data collected during 2015 of various locations on King Street. See the [Volumes Open Data King Street Pilot](volumes#king-street-pilot) section for more info.
- [King St. Transit Pilot – Detailed Traffic & Pedestrian Volumes](https://open.toronto.ca/dataset/king-st-transit-pilot-detailed-traffic-pedestrian-volumes/) contains 15 minute aggregated TMC data collected from Miovision cameras during the King Street Pilot. The counts occurred at 31-32 locations at or around the King Street Pilot Area. See the [Miovision Open Data](miovision#open-data) section for more info.
- [King St. Transit Pilot - Traffic & Pedestrian Volumes Summary](https://open.toronto.ca/dataset/king-st-transit-pilot-traffic-pedestrian-volumes-summary/) is a monthly summary of the above data, only including peak period and east-west data. The data in this dataset goes into the [King Street Pilot Dashboard](https://www.toronto.ca/city-government/planning-development/planning-studies-initiatives/king-street-pilot/data-reports-background-materials/). See the [Miovision Open Data](miovision#open-data) section for more info.
