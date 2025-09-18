# Traffic Volume <!-- omit in toc -->

Traffic volumes count different modes of traffic (bicycles, pedestrians, vehicles) typically conducted along a particular road, path, or at an intersection. A traffic count is commonly undertaken either automatically (with the installation of a temporary or permanent electronic traffic recording device), or manually. The short term data counts are either collected continuously or in different intervals.
 
The catalogue of the volume datasets owned, managed, and used by Transportation Data & Analytics is also available on the [intranet page](http://insideto.toronto.ca/transportation/data-analytics.htm#traffic-volumes) for City staff.

## Table of Contents <!-- omit in toc -->

- [Short Term Counts](#short-term-counts)
  - [Automatic Traffic Recorder (ATR) Counts](#automatic-traffic-recorder-atr-counts)
  - [Turning Movement Counts (TMC)](#turning-movement-counts-tmc)
  - [Other Methods](#other-methods)
  - [Cycling Counts](#cycling-counts)
- [Permanent Sources of Volume Data](#permanent-sources-of-volume-data)
  - [Vehicle Detector Stations (VDS)](#vds)
    - [RESCU Vehicle Detection Stations](#rescu-vehicle-detection-stations)
    - [Intersection Detectors](#intersection-detection)
    - [Blue City / Smartmicro](#blue-city--smartmicro-sensors)
  - [Miovision Cameras](#miovision-cameras)
  - [Watch Your Speed (WYS) Signs](#watch-your-speed-wys-signs)
- [What the Data are Used For](#what-the-data-are-used-for)
- [Open Data](#open-data)
  - [King Street Pilot](#king-street-pilot)
  - [Short Term Counts](#short-term-counts-1)

## Short Term Counts

Short term traffic count programs are traffic monitoring programs that collect traffic data for a short period of time, typically ranging from a few hours to several days. These programs use various methods to count and analyze vehicle traffic on a particular roadway or intersection.

### Automatic Traffic Recorder (ATR) Counts

- **Description:** ATR counts are primarily used to capture the volume of vehicles that travel on a roadway over a given period of time.
- **Data collection method:** ATR counts have traditionally been collected on ad hoc basis using road tubes, which use pneumatic technology to capture data that is later analyzed to estimate the count. As data collection methods continue to evolve, more firms and cities are choosing safer and more reliable tools to collect this type of data. Automated counters are used to gather various types of vehicular traffic data such as total vehicle volume, classification (type, size), and speed.
- **Collected data:** Vehicle class, volume and other data collection characteristics can be customized based on the survey requirements.
- Collected in 24-hour increments, from 1 day to 2 weeks
- Usually 72 hours (3 days) or 168 hours (7 days)
  - **Bins:** counted in 15-minute increments
  - **Volume:** the number of vehicles crossing the pneumatic tubes
  - **Speed:** the number of vehicles crossing the pneumatic tubes, separated into a matrix of speed bins (1-19kph, 20-25kph, 26-30kph, 31-35kph, etc.)
  - **Mode/ vehicle classification:** the number of vehicles crossing the pneumatic tubes, separated by vehicle classification (usually cars and trucks), according to the FHWA classification
- **Date range and updates:** January 5, 1993 to present; ad hoc updates
- **Geographic coverage:** city-wide, where ad hoc counts have been conducted. See the internal application [MOVE](https://move.intra.prod-toronto.ca/view/) for available dates and locations.
- **Format available:** CSV or PDF
- **Data management and access:** ATR  data collection is coordinated and managed by Data and Analytics- Data Collection team. 
- Data is available on [MOVE](https://move.intra.prod-toronto.ca/view/) internal application to City staff and contractors.
- More detailed data is available in the `traffic` Schema.
- Refer to [Short Term Count Readme](short_term_counting_program/README.md) for more details 


### Turning Movement Counts (TMC)

- **Description:** One of the most sought-after traffic data types is the TMC, also known as an Intersection Count. In a TMC, vehicle movements (e.g., left, through, and right turns) and volumes for all legs of the intersection are captured, for a specific period of time.
- **Data collection method:** Turning movement counts traditionally were conducted using person-recorders that manually observe and record the data or by deploying technology such a cameras. Now, they are collected using video-based collection.
- The use of any technology or innovative methods should be tested for accuracy prior to being an acceptable method to collect turning movement count data
- **Collected data**: vehicles are counted by their turning movements (left, through, right, u-turn) by direction of approach; bicycles (both street and on crosswalk) are counted by their turning movements (through only) by direction of approach; pedestrians are counted by the leg of the intersection crossed
  - **Bins**: counted in 15-minute increments
  - **Modes**: vehicles (cars, trucks, busses), cyclists, pedestrians, other active transportation modes 
 - **Date range and updates:** January 5, 1993 to present; ad hoc updates
- **Geographic coverage:** city-wide, where ad hoc counts have been conducted. See [MOVE](https://move.intra.prod-toronto.ca/view/) for available dates and locations.
- **Format available:** CSV or PDF
- **Data management and access:** TMC data collection is coordinated and managed by Data and Analytics- Data Collection team. They can be reached at TrafficData@toronto.ca.
- 8-hour count data is available on [Open Data](https://open.toronto.ca/dataset/traffic-volumes-at-intersections-for-all-modes/) to the public and also are available on [MOVE](https://move.intra.prod-toronto.ca/view/) internal application to City staff and contractors. 
- 8-hour data are also available in the `traffic` schema to internal users
- Refer to  [Short Term Count Readme](short_term_counting_program/README.md) for more details

### Other Methods

Here are some examples of other short term traffic counting programs conducted by the City which are less commonly used.
- **Pedestrian Delay and Classification Study:** Through manual observation, this study captures pedestrians crossing a road within defined zones.
- **Pedestrian Crossover Observation Study (PXO):** This study observes the behaviour of pedestrians using a crosswalk.

### Cycling Counts

The Cycling & Pedestrian Projects Unit (formerly Cycling Team) conducted their own short term counts. The data structure is documented in the [`cycling_unit`](cycling_unit/) folder. 

## Permanent Sources of Volume Data

Permanent traffic counts refer to the process of continuously monitoring and recording the volume and characteristics of vehicular traffic on a particular road, highway or at intersection using a permanent automated counting system. These systems typically consist of sensors, cameras, or other devices that are installed along the roadway or at intersections to collect data on traffic volumes, speed, classification, and other parameters.

### VDS

The city operates various permanent Vehicle Detector Stations (VDS), employing different technologies, including RESCU, intersection detectors, Blue City and Smartmicro:

#### RESCU Vehicle Detection Stations

- **Description:** Road Emergency Services Communication Unit (RESCU) track traffic volume mostly on expressways using loop detectors and radar. 
- **Data collection method:** Loop detectors installed on the ground of the road surface OR Radar detectors (which function the same way as loop detectors) placed on the roadside.  
- **Collected data:**
  - **Bins**: Raw data is recorded in 20 second increments and stored both as raw and 15 minute aggregated bins by D&A. 
  - **Modes:** Vehicular traffic is detected by RESCU.
 - **Date range and updates:** January 2017 to present; daily updates
- **Geographic coverage:** Highways and Expressways managed by the City of Toronto
- **Format available:** Transnomis Postgres Database
- **Data management and access:** Data is managed and coordinated by ITS Central.
  - Active Traffic Management- ITS central and Data & Analytics units are the main contacts.
  - Aggregated data is available in vds schema.
  - More information can be found on [VDS Readme](vds/readme.md#division_id2).

#### Intersection Detection
- Geographically disperse intersection detectors located throughout the city with questionable data quality. 
- More information can be found in the VDS Readme under the [division_id 8001](vds/readme.md#division_id8001) heading.

#### Blue City / Smartmicro Sensors
- Two different sensor technologies installed as a limited pilot along Yonge St and around the Rogers Centre (Lakeshore/Spadina).
- More information can be found on [VDS Readme](vds/readme.md#division_id2).

### Miovision Cameras

- **Description:**  Miovision cameras are installed at selected intersections across the City. Camera feeds are converted to turning movement count volumes for different modes (truck, bus, car,   pedestrian, bicycle). Miovision then processes the video footage and provides volume counts in aggregated 1 minute bins
- **Data collection method:** Data is collected using Permanent counters - Miovision cameras
- **Collected data:**
-  24/7 volume data at these locations are available on an ongoing basis
   - **Bins**: Data are aggregated from 1-minute volume data into two types of 15-minute volume products: Turning Movement Count (TMC) and Automatic Traffic Recorder (ATR) equivalents.
   - **Modes:** truck, bus, car, pedestrian, bicycle, although bicycle volumes are less accurate, and are actively being improved. 
- **Date range and updates:** January 2019 to present; daily updates
- **Geographic coverage:** Limited permanent counter locations (60 active locations as of April 24, 2023) 
- **Format available:** Custom data extract in CSV
- **Data management and access:** 
- Miovision data collection is coordinated and managed by Data and Analytics unit. 
- More information can be found on [Miovision Readme](miovision/README.md)

### Watch Your Speed (WYS) Signs

- **Description:**  The city has installed [Watch Your Speed Signs](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-initiatives/initiatives/watch-your-speed-program/) that use a radar speed detection device and an LED display to  display the speed a vehicle is travelling at and flashes if the vehicle is travelling over the speed limit. The signs collect these speed observations and send them to the cloud. Processed data aggregated to 1-hour and 5 km/h bins
- Installation of the sign was done as part of 2 programs: 
  - The permanent watch your speed sign program including school watch your speed which has signs installed at high priority schools. As part of the [Vision Zero Road Safety Plan](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/), these signs aim to reduce speeding.
  - Mobile watch your speed which has signs mounted on trailers that move to a different location every few weeks, 
.
- **Data collection method:** The City API script can grab data from each watch your speed sign the city has. It mainly pulls speed and volume data. The API supports more calls including setting/getting the schedule, setting/getting the messages each sign displays and other calls.
- **Collected data:**
  - **Bins**: Raw data table is pre-aggregated into 5 minute bin. D&A processes and aggregates data to 1-hour and 5 km/h bins.
  - **Modes:** Vehicular traffic (e.g. cars, trucks, buses, motorcycles).The signs are generally not designed to detect bicycles or pedestrians but they can be triggered by and detect these passing road users. 
- **Date range and updates:** Data is available at some locations since January 2016 
- **Geographic coverage:** City Wide- permanent signs are mostly in school zones. The number of mobile signs varies 
- **Format available:** Custom data extract in CSV
- More information can be found on  [WYS Readme](../wys/readme.md).

## What the Data are Used For

Traffic count data is used for a variety of purposes, including but not limited to:
- **Annual Average Daily Traffic (AADT)** : Traffic counts provide the source data used to calculate AADT, which is the common indicator used to represent traffic volume. 
  - An AADT count identifies the average vehicle volumes in a 24-hour period. That value is produced by dividing the total vehicle volume for one year by 365 days. AADT counts are a good indicator of how busy a road is and is often used for evaluating, selecting or designing a new facility or territory. AADT values can be calculated from short duration counts using a method described in the FHWA Traffic Monitoring Guide.
- **Planning and design of transportation systems**: Traffic data is used to assess current and past volume and performance and to predict future volume and performance on a roadway or at an intersection, which can help transportation planners and engineers design and improve transportation systems.
- **Safety analysis**: 
  - Traffic count data can be used to identify crash rates, and help safety engineers develop countermeasures to improve safety on the roadway.
  - Traffic counts that include speeds are used in speed limit enforcement efforts, highlighting peak speeding periods to optimise speed camera use and educational efforts and traffic safety analysis.
  - The use of non-motorized travel data and information supports analysis regarding the impacts to the transportation network (on volumes and safety) resulting from the use of bicycles as an alternative travel method
- **Environmental impact analysis**: Traffic count data can be used to estimate emissions from vehicles, and assess the impact of transportation on air quality and noise pollution.
- **Economic analysis**: Traffic count data can be used to estimate the economic impact of transportation on a region, such as the number of jobs supported by transportation infrastructure or the amount of revenue generated by transportation-related businesses.
- **Traffic management**: TMC data is also collected to find the intersection’s level of service. TMC data is used in other types of analysis related to the overall performance of an intersection too. It can be used to optimize traffic signal timings, manage traffic flow during special events or emergencies, and plan detours for road construction projects
- **Land use planning** : Traffic count data can be used to help city planners make decisions about zoning and land use patterns, such as determining the appropriate mix of commercial and residential development.
- **Other uses of traffic data** include project and resource allocation programming;operations and emergency evacuation; capacity and congestion analysis; traffic forecasts; project evaluation; pavement design;  cost allocation studies; estimating the economic benefits of highways; preparing vehicle size and weight enforcement plans; freight movement activities; pavement and bridge management systems, etc.

## Open Data

### King Street Pilot

For the King Street Transit Pilot, the below volume datasets were released. The first is [ATR counts](#automated-traffic-recorders-atrs) from before the start of the pilot, tagged to the City's Centreline layer, while the other two are from [Miovision](miovision) permanent count cameras and are georeferenced by intersection:

- [King St. Transit Pilot - 2015 King Street Traffic Counts](https://open.toronto.ca/dataset/king-st-transit-pilot-2015-king-street-traffic-counts/) contains 15 minute aggregated ATR data collected during 2015 of various locations on King Street. [Here](sql/open_data-ksp_atr_2015.sql) is the SQL that generated that table.
- [King St. Transit Pilot – Detailed Traffic & Pedestrian Volumes](https://open.toronto.ca/dataset/king-st-transit-pilot-detailed-traffic-pedestrian-volumes/) contains 15 minute aggregated [TMC](#turning-movement-counts-tmcs) data collected from [Miovision](volumes/miovision) readers during the King Street Pilot. The counts occurred at 31-32 locations at or around the King Street Pilot Area ([SQL](miovision\sql\open_data_views.sql)).
- [King St. Transit Pilot - Traffic & Pedestrian Volumes Summary](https://open.toronto.ca/dataset/king-st-transit-pilot-traffic-pedestrian-volumes-summary/) is a monthly summary of the above data, only including peak period and east-west data ([SQL](miovision\sql\open_data_views.sql)). The data in this dataset goes into the [King Street Pilot Dashboard](https://www.toronto.ca/city-government/planning-development/planning-studies-initiatives/king-street-pilot/data-reports-background-materials/)

### Short Term Counts

- 8-hour count data [Turning Movement Count](#turning-movement-counts-tmc) data are available on [Open Data](https://open.toronto.ca/dataset/traffic-volumes-at-intersections-for-all-modes/) 
