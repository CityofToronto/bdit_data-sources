

# **GENERAL INFORMATION**




# **Traffic Volume Counts**



## **Description**
 Traffic count is a count of traffic by different modes which is conducted along a particular road, path, or intersection. A traffic count is commonly undertaken either automatically (with the installation of a temporary or permanent electronic traffic recording device), or manually. The most common traffic volume counts are either temporary  or permanent. The short term data counts are either collected continuously or in different intervals.
 
 ## **Short Term Counts**
Short term traffic count programs are traffic monitoring programs that collect traffic data for a short period of time, typically ranging from a few hours to several days. These programs use various methods to count and analyze vehicle traffic on a particular roadway or intersection.
### **Automatic Traffic Recorder (ATR) counts**

- **Description:** ATR counts are primarily used to capture the volume of vehicles that travel on a roadway over a given period of time.
- **Data collection method:** ATR counts have traditionally been collected with road tubes, which use pneumatic technology to capture data that is later analyzed to estimate the count. As data collection methods continue to evolve, more firms and cities are choosing safer and more reliable tools to collect this type of data. - Automated counters are used to gather various types of vehicular traffic data such as total vehicle volume, classification (type, size), and speed.
- **Collected data:** Vehicle class, volume and other data collection characteristics can be customized based on the survey requirements.
- Collected in 24-hour increments, from 1 day to 2 weeks
- Usually 72 hours (3 days) or 168 hours (7 days)
  - **Bins:** counted in 15-minute increments
  - **Volume:** the number of vehicles crossing the pneumatic tubes
  - **Speed:** the number of vehicles crossing the pneumatic tubes, separated into a matrix of speed bins (1-19kph, 20-25kph, 26-30kph, 31-35kph, etc.)
  - **Mode/ vehicle classification:** the number of vehicles crossing the pneumatic tubes, separated by vehicle classification (usually cars and trucks), according to the FHWA classification
- **Data management and access** ATR  data collection is coordinated and managed by Data and Analytics- Data Collection team. They can be reached at TrafficData@toronto.ca.
- Data is available on [MOVE](https://move.intra.prod-toronto.ca/view/) website.
- More detailed data is available in Traffic Schema.
- Refer to [Short Term Count Readme](short_term_counting_program/README.md) for more details 

Data   Source | Data Product | Description | Date Range & Updates | Geographic Coverage | Formats Available | How to Access
-- | -- | -- | -- | -- | -- | --
Pneumatic   Road Tubes (ATR Counts),  | Vehicle Volume ATR Reports, Speed / Volume ATR   Reports | Data are collected on  an ad hoc basis using pneumatic road tubes, which count the volume of  vehicles along a road segment. Speed Volume ATR data are grouped  into speed buckets. Data are collected in 24-hour periods. Data are available in standard reports — summary reports and detailed data in   15-minute intervals. | January 5, 1993 to   present; ad hoc updates | City-wide, where ad hoc   counts have been conducted. See [MOVE](https://move.intra.prod-toronto.ca/view/) for available dates and locations. | CSV or PDF | [MOVE](https://move.intra.prod-toronto.ca/view/), accessible to City   staff and contractors. Detailed data are in Traffic schema available to internal users 




### **Turning Movement Counts (TMC)**
- **Description:** One of the most sought-after traffic data types is the TMC, also known as an Intersection Count. In a TMC, vehicle movements (e.g., left, through, and right turns) and volumes for all legs of the intersection are captured, for a specific period of time.
- **Data collection method:** Turning movement counts can be conducted using person-recorders that manually observe and record the data or by deploying technology such a cameras.
- The use of any technology or innovative methods should be tested for accuracy prior to being an acceptable method to collect turning movement count data
- **Collected data**: vehicles and bicycles are counted by their turning movements (left, through, right, u-turn) by direction of approach; pedestrians are counted by the leg of the intersection crossed
  - **Bins**: counted in 15-minute increments
  - **Modes**: vehicles (cars, trucks, busses), cyclists, pedestrians, other active transportation modes 
- **Data management and access**  TMC data collection is coordinated and managed by Data and Analytics- Data Collection team.  They can be reached at TrafficData@toronto.ca.
- Data is available on [MOVE](https://move.intra.prod-toronto.ca/view/) website.
- More detailed data is available in Traffic Schema.  
- Refer to  [Short Term Count Readme](short_term_counting_program/README.md) for more details  for more details  


Data Source | Data Product | Description | Date Range & Updates | Geographic Coverage | Formats Available | How to Access
-- | -- | -- | -- | -- | -- | --
Turning Movement Count (TMC) |Turning Movement Count Reports |Through video-based collection, this study counts the vehicles, cyclists, and pedestrians moving through an intersection. Complete data is available in Traffic schema and 8 hours of multi-modal turning movement data are available on [MOVE](https://move.intra.prod-toronto.ca/view/) website. For each count, data are available in standard reports — summary reports and detailed data in 15-minute intervals. | January 3, 1984 to present; ad hoc updates | City-wide, where ad hoc counts have been conducted.  See [MOVE](https://move.intra.prod-toronto.ca/view/) for available dates and locations. | CSV or PDF | [MOVE](https://move.intra.prod-toronto.ca/view/), accessible to City staff and contractors  For complete 14-hour count data, please contact TrafficData@toronto.ca.Detailed data are available in Traffic schema to internal users
### **Other methods**
Here are some examples of other short term traffic counting programs conducted by the City which are less commonly used.
- **Pedestrian Delay and Classification Study:** Through manual observation, this study captures pedestrians crossing a road within defined zones.
- **Pedestrian Crossover Observation Study (PXO):** This study observes the behaviour of pedestrians using a crosswalk.

## **Permanent Counts**

### **Loop Detectors RESCU**
- **Description:** Road Emergency Services Communication Unit (RESCU) track traffic volume mostly on expressways using loop detectors. 
- **Data collection method:** Loop detectors installed on the ground of the road surface.
- Radar detectors (which function the same way as loop detectors) placed on the roadside. 
-**Collected data:**
  - **Bins**: Raw data is recorded in 20 second increments. Data handled by D&A is aggregated to 15 minute increments and produced as reports out of Oracle. 
  - **Modes:** Vehicular traffic is detected by RESCU.
- **Data management and access:** Data is managed and coordinated by ITS Central.  Active Traffic Management- ITS central and Data & Analytics units are the main contacts .
- Aggregated data is available in rescu schema.
- More information can be found on [RESCU Readme](rescu/README.md)

Data   Source | Data Product | Description | Date Range & Updates | Geographic Coverage | Formats Available | How to Access
-- | -- | -- | -- | -- | -- | --
RESCU | Custom Data Extracts | Road Emergency   Services Communication Unit (RESCU) track traffic volume mostly on   expressways using loop detectors. loop detectors installed on the ground of the Road surface. Radar detectors (which function the same way as loop detectors) placed on the roadside. | January 2017 to present;   daily updates | Highways and Expressways managed by the City of Toronto  | CSV | Please contact   transportationdata@toronto.ca     Detailed data is available to internal staff in RESCU shema



### **Miovision-Cameras**
- **Description:**  Miovision currently provides volume counts gathered by cameras installed at specific intersections. Miovision then processes the video footage and provides volume counts in aggregated 1 minute bins
- **Data collection method:** Data is collected using Permanent counters - Miovision cameras
- **Collected data:**
-  24/7 volume data at these locations are available on an ongoing basis
   - **Bins**: Data are aggregated from 1-minute volume data into two types of 15-minute volume products: Turning Movement Count (TMC) and Automatic Traffic Recorder (ATR) equivalents.
   - **Modes:** truck, bus, car,   pedestrian, bicycle), although bicycle volumes are less accurate, and are actively being improved. 
- **Data management and access** 
- Miovision data collection is coordinated and managed by Data and Analytics unit. Detailed data is in Miovision schema available to internal users- 
- More information can be found on [Miovision Readme](miovision/README.md)

Data   Source | Data Product | Description | Date Range & Updates | Geographic Coverage | Formats Available | How to Access
-- | -- | -- | -- | -- | -- | --
Permanent   Counters - Miovision Cameras- | Custom Data Extracts | Miovision cameras are installed at selected locations across the City. Camera feeds are converted to turning movement count volumes for many modes (truck, bus, car,   pedestrian, bicycle) | January 2019 to present;   daily updates | Limited permanent   counter locations (60 active locations as of April 24, 2023) | CSV | Please contact   TransportationData@toronto.ca. Detailed data is in  Miovision Schema available to internal users

### Watch Your Speed (WYS)
- **Description:**  The city has installed [Watch Your Speed Signs](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-initiatives/initiatives/watch-your-speed-program/) that display the speed a vehicle is travelling at and flashes if the vehicle is travelling over the speed limit. The signs also collect volume and speed data.
- Installation of the sign was done as part of 3 programs: 
  - the normal watch your speed sign program,
  - mobile watch your speed which has signs mounted on trailers that move to a different location every few weeks, 
  - and school watch your speed which has signs installed at high priority schools. As part of the [Vision Zero Road Safety Plan](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/), these signs aim to reduce speeding.
- **Data collection method:** The City API script can grab data from each watch your speed sign the city has. It mainly pulls speed and volume data. The API supports more calls including setting/getting the schedule, setting/getting the messages each sign displays and other calls.
- **Collected data:**
  - **Bins**: Raw data table is pre-aggregated into roughly 5 minute bin. D&A processes and aggregates data to 1-hour and 5 km/h bins.

  - **Modes:** Vehicular traffic (e.g. cars, trucks, buses, motorcycles).The signs are generally not designed to detect bicycles or pedestrians but they can be triggered by and detect these passing road users. 
- **Data management and access:** 
- More information can be found on  [WYS Readme](../wys/readme.md).

Data   Source | Data Product | Description | Date Range & Updates | Geographic Coverage | Formats Available | How to Access
-- | -- | -- | -- | -- | -- | --
Watch   Your Speed | Custom Data Extracts | City of Toronto's "Watch Your Speed"  signs uses devices called speed display signs or driver feedback signs which contain a radar speed detection device and an LED display. Processed data aggregated to   1-hour and 5 km/h bins using the |  Data is available at some locations since January 2016  | City Wide- permanent signs are mostly in school zones. The number of mobile signs varies | CSV | Please contact   transportationdata@toronto.ca     Detailed data is in WYS schema available to internal staff

## **What do we use this for**
Traffic count data is used for a variety of purposes, including but not limited to:
- **Annual Average Daily Traffic (AADT)** : Traffic counts provide the source data used to calculate AADT, which is the common indicator used to represent traffic volume. 
  - An AADT count identifies the average vehicle volumes in a 24-hour period. That value is produced by dividing the total vehicle volume for one year by 365 days. AADT counts are a good indicator of how busy a road is and is often used for evaluating, selecting or designing a new facility or territory. AADT values can be calculated from short duration counts using a method described in the FHWA Traffic Monitoring Guide.
- **Planning and design of transportation systems**: Traffic data is used to assess current and past volume and performance and to predict future volume and performance on a roadway or at an intersection, which can help transportation planners and engineers design and improve transportation systems.
- **Safety analysis**: 
  - Traffic count data can be used to identify crash rates, and help safety engineers develop countermeasures to improve safety on the roadway.
  - Traffic counts that include speeds are used in speed limit enforcement efforts, highlighting peak speeding periods to optimise speed camera use and educational efforts and traffic safety analysis.
  - The use of nonmotorized travel data and information supports analysis regarding the impacts to the transportation network (on volumes and safety) resulting from the use of bicycles as an alternative travel method
- **Environmental impact analysis**: Traffic count data can be used to estimate emissions from vehicles, and assess the impact of transportation on air quality and noise pollution.
- **Economic analysis**: Traffic count data can be used to estimate the economic impact of transportation on a region, such as the number of jobs supported by transportation infrastructure or the amount of revenue generated by transportation-related businesses.
- **Traffic management**: TMC data is also collected to find the intersection’s level of service. TMC data is used in other types of analysis related to the overall performance of an intersection too. It can be used to optimize traffic signal timings, manage traffic flow during special events or emergencies, and plan detours for road construction projects
- **Land use planning** : Traffic count data can be used to help city planners make decisions about zoning and land use patterns, such as determining the appropriate mix of commercial and residential development.
- **Other uses of traffic data** include project and resource allocation programming; performance reporting; operations and emergency evacuation; capacity and congestion analysis; traffic forecasts; project evaluation; pavement design; safety analyses; emissions analysis; cost allocation studies; estimating the economic benefits of highways; preparing vehicle size and weight enforcement plans; freight movement activities; pavement and bridge management systems; and signal warrants, air quality conformity analysis, etc.


