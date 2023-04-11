


# **GENERAL INFORMATION**




# **Traffic Volume Counts**



## **Description**
 Traffic count is a count of traffic by different modes which is conducted along a particular road, path, or intersection. A traffic count is commonly undertaken either automatically (with the installation of a temporary or permanent electronic traffic recording device), or manually. The most common traffic volume are either temporary  or permanent. The short term data counts are either collected continuously or in different intervals.
 
 ## **Short Term Counts**

### **Automatic Traffic Recorder (ATR) counts**

- ATR counts are primarily used to capture the volume of vehicles that travel on a roadway over a given period of time. ATR counts have traditionally been collected with road tubes, which use pneumatic technology to capture data that is later analyzed to estimate the count. As data collection methods continue to evolve, more firms and cities are choosing safer and more reliable tools to collect this type of data.
- Automated counters are used to gather various types of vehicular traffic data such as total vehicle volume, classification (type, size), and speed. Data is collected with pneumatic road tubes or with video cameras in locations unsuited for road tube use. Vehicle class, volume and other data collection characteristics can be customized based on your survey requirements.
- Raw data is usually recorded in 15 minute increments.
- Refer to [Flow Readme](https://github.com/CityofToronto/bdit_data-sources/blob/master/volumes/README.md) for more details 
- ATR  data collection is coordinated and managed by Data and Analytics- Data Collection team
- Data is available on [MOVE](https://move.intra.prod-toronto.ca/view/) website.
- More detailed data is available in Traffic Schema.

### **Turning Movement Counts (TMC)**
- One of the most sought-after traffic data types is the TMC, also known as an Intersection Count. In a TMC, vehicle movements (e.g., left, through, and right turns) and volumes for all legs of the intersection are captured, for a specific period of time.

- Turning movement counts can be conducted using person-recorders that manually observe and record the data or by deploying technology such a cameras. R
The use of any technology or innovative methods should be tested for accuracy prior to being an acceptable
method to collect turning movement count data

- Raw data is recorded in 15 minute increments.
- TMC data collection is coordinated and managed by Data and Analytics- Data Collection team
- Data is available on [MOVE](https://move.intra.prod-toronto.ca/view/) website.
- More detailed data is available in Traffic Schema.
- Refer to [Flow Readme](https://github.com/CityofToronto/bdit_data-sources/blob/master/volumes/README.md) for more details  

 ## **Permamnt  Counts**

### **Loop Detectors RESCU**
- Road Emergency Services Communication Unit (RESCU) track traffic volume mostly on expressways using loop detectors. 

- Loop detectors installed on the ground of the road surface.
- Radar detectors (which function the same way as loop detectors) placed on the roadside. 
- Data is coordinated by ITS Central.
- Raw data is recorded in 20 second increments. 
- Data handled by D&A is aggregated to 15 minute increments and produced as reports out of Oracle. 
- Aggregated data is available in rescu schema.
- More information can be found on the city's website or [RESCU Readme](https://github.com/CityofToronto/bdit_data-sources/blob/master/volumes/rescu/README.md)



### **Miovision-Cameras**
- Miovision currently provides volume counts gathered by cameras installed at specific intersections. Miovision then processes the video footage and provides volume counts in aggregated 1 minute bins
- You can see the current locations of Miovision cameras on this map (need to update the link).
- Data are aggregated from 1-minute volume data into two types of 15-minute volume products: Turning Movement Count (TMC) and Automatic Traffic Recorder (ATR) equivalents. 
- Miovision sata collection is coordinated and managed by (need to insert)
- More information can be found on [Miovision Readme](https://github.com/CityofToronto/bdit_data-sources/blob/master/volumes/miovision/README.md)

### Watch Your Speed (WYS)
- The city has installed [Watch Your Speed Signs](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-initiatives/initiatives/watch-your-speed-program/) that display the speed a vehicle is travelling at and flashes if the vehicle is travelling over the speed limit.
- Installation of the sign was done as part of 3 programs: 
  - the normal watch your speed sign program,
  - mobile watch your speed which has signs mounted on trailers that move to a different location every few weeks, 
  - and school watch your speed which has signs installed at high priority schools. As part of the [Vision Zero Road Safety Plan](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/), these signs aim to reduce speeding.
- More information can be found on  [WYS Readme](https://github.com/CityofToronto/bdit_data-sources//wys/readme.md).
### **Other methods**

## **What do we use this for**
- Traffic counts provide the source data used to calculate the Annual Average Daily Traffic (AADT), which is the common indicator used to represent traffic volume.TMCs are typically used for traffic modeling, as well as to help determine an intersection’s capacity and provide data insights to inform signal retiming. 

- TMC data is also collected to find the intersection’s level of service. TMC data is used in other types of analysis related to the overall performance of an intersection too. Traffic counts are useful for comparing two or more roads, and can also be used alongside other methods to find out where the central business district (CBD) of a settlement is located.
-  Traffic counts that include speeds are used in speed limit enforcement efforts, highlighting peak speeding periods to optimise speed camera use and educational efforts and traffic safety analysis

- Traffic data and information are needed to assess current and past performance and to predict future performance. Improved traffic data, including data on ramps, are needed for reporting in the Highway Performance Monitoring System (HPMS), and there are now opportunities to utilize traffic data from Intelligent Transportation Systems (ITS) to support coordination of planning and operations functions at the Federal and State levels. 

- The use of nonmotorized travel data and information supports analysis regarding the impacts to the transportation network (on volumes and safety) resulting from the use of bicycles as an alternative travel method.
- Uses of traffic data include project and resource allocation programming; performance reporting; operations and emergency evacuation; capacity and congestion analysis; traffic forecasts; project evaluation; pavement design; safety analyses; emissions analysis; cost allocation studies; estimating the economic benefits of highways; preparing vehicle size and weight enforcement plans; freight movement activities; pavement and bridge management systems; and signal warrants, air quality conformity analysis, etc.

### **Annual Average Daily Traffic (AADT) counts**
An AADT count identifies the average vehicle volumes in a 24-hour period. That value is produced by dividing the total vehicle volume for one year by 365 days. AADT counts are a good indicator of how busy a road is and is often used for evaluating, selecting or designing a new facility or territory. AADT values can be calculated from short duration counts using a method described in the FHWA Traffic Monitoring Guide.

