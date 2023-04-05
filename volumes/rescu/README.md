> Please each time that you change or add a piece of information, mention the time of update and add keywords about changes. 

This readme file was generated on 2023-04-04 by Shahrzad Borjian.

This readme file was Updated on [YYYY-MM-DD] by [NAME] with [changes keywords].



# GENERAL INFORMATION
## Keywords
>RESCU, traffic, volume, long_term, Data and Analytics



## **Traffic RESCU**
Road Emergency Services Communication Unit (RESCU) track traffic volume on expressways using loop detectors. More information can be found on the [city's website](https://www.toronto.ca/services-payments/streets-parking-transportation/road-restrictions-closures/rescu-traffic-cameras/) or [here](https://en.wikipedia.org/wiki/Road_Emergency_Services_Communications_Unit).

Raw data is available in `rescu.raw_15min` whereas processed 15-min data is available in `rescu.volumes_15min`.

![image](https://user-images.githubusercontent.com/98347176/228930515-3f38a7a0-d2be-444b-b70d-1a6eb79ad74a.png)

## **Description**:


## What do we use this for/ data usage:
	
### 1- What is the purpose of using the data? (Primary or secondary)	
>Real time data reporting as a primary use for Traffic Management Centre (TMC). Analytics as fulfilled by data specialists as part of data requests. 
### 2- Who uses this data within D&A?  What tasks is data used for?	
>Data specialists. Used for data requests (mostly staff asking for volumes on highways).   

### 3- Who uses this data outside D&A? 
> TMC. Only used for real time data reporting. 

### 4- What are the limitations with using this data based the above uses?	
>Data gaps make data unreliable for data requests.  

### 5- Does this data get published?	
>No. Data could be published but at this current point it is not. 


## **Contact information**:
### 1- Who is the internal contact (within D&A) to reach out to with questions/concerns?
> Unclear. Staff who created pipeline are no longer with the team and there has been no official handover to another individual. May potentially fall under the purview of specialists within Data Operations.

### 2- Who is the external contact (outside D&A) to reach out to with questions/concerns?	
> Active Traffic Management.  ATM also redirects inquires to Jim and previously to Steven Bon. 




## **Date of data collection**: 

> ongoing: data availble since

> regular updates



## **Geographic location of data collection**:
>Loations can be found on the [city's website](https://www.toronto.ca/services-payments/streets-parking-transportation/road-restrictions-closures/rescu-traffic-cameras/) 

> Provide CRS/SRID (e.g. WGS84/EPSG:4326)

## **Data Ownership**
	
#### 1- Who is responsible for this data within D&A?
>Jesse. 

### 2- Who is responsible for this data outside D&A?
>TMC. Active Traffic Management. As ITS Central is becoming of more use (as field servers are no longer working and ITS Central is the replacement) then responsibility may shift to Transnomis. 

### 3- Is data housed with external contractors or consultants?
> Real time data use is accessed through ITS Central and thus data is housed with Transnomis.  

## Support for data collection
> information about who supported the collection of the data (program/unit): 


# SHARING/ACCESS INFORMATION
## Restrictions placed on the data: 
> list any restrictions on data access (public data, internal data (team, unit, division, the City, a specific group pf people))



## Internal access to the data
> any process to access the data (requires registration, aquire permission, a link, letting someone know)

> provide the information of how internal team (Data & Analytics un) can find the data:
server name, schema, database, version, admin, 



## Licenses placed on the data (Privacy concerns)
> provide the link to thye known licences (e.g. CC)

> if it's not a known licence, list the concerns and limitations due to the licensing



## Limitation of the data
>Data gaps make data unreliable for data requests. 



## Links to any use the data: 
>TBA



## Links to other publicly accessible locations of the data: 
>TBA



## Links/relationships to supplimentary data sets:
> like centreline for studies/collisions
>TBA




## Do you expect citation for this dataset: 
> TBA




# DATA & FILE OVERVIEW
## List files
> if the dataset or a version of it is stored in other places, list them here and provide information versioning information



## Relevant datasets
> if to work with the data, we need to access another dataset/ or there are relevant dataset, list them here with a link (schema/ folder address)



## Format of accessible data
> the final ready format 
> what would be the first and easy way to access disaggregated data (low effort for us)



## Available formats 
> what format the data was collected in (original format)?
> other formats that data is available: map, txt, geojson, excel, shapefile



## Versions of the data
> if yes, 
1. name of file(s) that was updated: 
2. Why was the file updated? 
3. When was the file updated? 

> use a table formatting (version id, created at, changes)



# METHODOLOGICAL INFORMATION
## **Description of methods used for collection/generation of data:**

### 1- How is the data collected?	
> Loop detectors installed on the ground of the road surface.
Radar detectors (which function the same way as loop detectors) placed on the roadside. 

### 2- What are the properties of the data? 	
>Volume data at specific highway locations

### 3- Is there an up to date map for this data? If yes, who should be updating the map?      
>- No, there is an internal web app [Created by D&A] but it is not helpful as it only shows locations of loop detectors and volume range by date but cannot be manipulated. There also significant data gaps which reduce the usability of this web app.
>- Electrical contractor provides data feed which included latitude and longitude, however there are locational errors. The overall accuracy of the spatial data provided is questionable. 
>- TPIM (Traffic Plant / Installation and Maintenance) would be responsible for updating this map as it falls within their purview. 
### 4- How often is the data updated?	
>Raw data feeds into oracle every 20 seconds. Data (Jim's aggregation) pushed into Oracle daily. Data Pipeline Flow and Transformation – 
## **Methods for processing the data: Data Pipeline Flow and Transformation- How does the data move/transform through the organization?**
	
### 1- How is the data organized and aggregated? 	
> Raw data is recorded in 20 second increments. Data handled by D&A is aggregated to 15 minute increments and produced as reports out of Oracle. Data is matched to arterycode (directional).Loop detectors, radar detectors have coordinates. 

### 2- Where is the raw data stored?	   	
>Raw data is stored in Oracle and ITS Central. 
Within ITS Central it is stored in Postgres database but we are unsure as to how much data is being stored in the system.
### 3- How is the raw data stored?	
> Unsure—particularly now that there are two streams (one in ITS Central and one in Oracle).

### 4- Where is the data stored?	
>Pulling data from databases into Postgres. Windows task scheduler is responsible for pulling the data from \\tssrv. 
>Refer to [readme_pull](https://github.com/CityofToronto/bdit_data-sources/blob/master/volumes/rescu/README_PULL.md) for more information

### 5- How is the data stored?	
>Unsure.   




## Instrument/device or software-specific information needed to interpret the data:
> Provide information on how data can be interpreted baed on collection device/instrument  



## Standards information, if appropriate: 
> N/A- to be added



## Environmental/experimental conditions: 
> N/A- to be added 



## Data Quality/ Describe any quality-assurance procedures performed on the data

### 1- Are there known data gaps/incomplete data? 	
>Yes, there are many data gaps.
### 2- What are they gaps?	
>Missing volumes due to detector issues. Data reports sent to D&A out of Oracle contain specific lane-level data. This may help isolate detectors that are down and help in closing gaps and validating data.

### 3- How are data gaps/incomplete data addressed? 
>Currently within D&A and for the purpose of Data Requests, requesters tell requestees that data is not available.  

### 3- Who is responsible for addressing data gaps/incomplete data?
>Unsure. By default it is D&A.

#### 4- Are there data quality assessment processes for the data?
>Yes, there is a pipeline check but it regularly fails so it may not a sufficient form of quality assessment.No upstream validation processes identified.

#### 5- How often are data quality assessment processes for the data undertaken? 
>D&A process done daily. QA process counts the number of rows that have data with 7000 rows being the threshold.   
 
#### 6- Are external contractors or consultants responsible for checking data quality?
> Unsure. 


## Data Validity- Is the data truly representative of the real world? 
	
### 1- Who is responsible for checking the validity of the data?	
>Unsure. 
It appears that validation is only done once data reaches D&A. Unclear is there are validation processes upstream. 

### 2- Who should be involved when there are inconsistencies identified with the data?	
>Unsure.
### 3- For external data sources: is data received previously validated? 	
>N/A
### 4- For external data sources: if data received is previously validated, how is it validated?
>N/A
### 5- Are external contractors or consultants responsible for checking data validity? Who is the City staff contact who works for that vendor?	
>N/A

## Data Maintainance 
## Maintenance - 
- (Hardware/Software)
	Hardware
	ITS Central
	Oracle Data 	
The following questions need to be answered in each stage in the data flow diagram:	
### 1- Who is responsible for the status of data functionality and the overall maintenance of the data collection? 	
>-  Hardware: TPIM is responsible. Typically Jim troubleshoots the problem first. 
>- ATM Active Traffic Management team uses the data but they do not deal with the hardware or functionality of the data. 
>- ITS Central: Transnomis (Simon) via Black/Mac is responsible.
>- Oracle DB Server: Jim is responsible as he functions as the owner of the dataset.

### 2- How often is data maintained? Are there monitoring mechanisms when parts of the data flow is not working?	
>Unsure. 
### 3- Is there a chain/process for when equipment goes wrong/breaks?	
> Unsure.
### 4- What is the process that needs to be undertaken when equipment goes wrong/breaks? 	> Unsure.
### 5-Who should be notified if something goes wrong/ there are changes to data? 	
>Data specialists within D&A. Data users within TMC. 

### 5- Are external contractors or consultants responsible for maintenance? Who is the City staff contact who works for that vendor?	
>ATM is leased to a contractor in the traffic control room. Contractor runs and manages the work orders. 
 



# DATA-SPECIFIC INFORMATION FOR:
## Schema
Data is stored in RESCU schema.
## Number of variables: 


>`raw_20sec` table:
"datetime_bin",
"detector_id",
"lane_no",
"volume",
"occupancy",
"speed",
"uid"

>`raw_15min` table:
"dt", 
"raw_info", 
"raw_uid"

>`volumes_15min` table:
"detector_id",
"datetime_bin",
"volume_15min",
"arterycode",
"volume_uid",


## Missing data: 
> TBA



## Specialized formats or other abbreviations used:
>TBA

## Other
> if there are information worth sharing but have not considered in this template, please list them here
