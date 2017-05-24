# Bluetooth - Bliptrack

## Table of Contents
1. [Overview](#1-overview)
2. [Table Structure](#2-table-structure)
3. [Technology](#3-technology)
4. [Data Processing](#4-data-processing)
5. [Bliptrack UI](#5-bliptrack-ui)
6. [Bliptrack API](#6-bliptrack-api)

## 1. Overview (Aakash)
- coverage
- what does the data look like?
The live feed, and archived data are available on the [Open Data Portal](http://www1.toronto.ca/wps/portal/contentonly?vgnextoid=0b3abcfaf9c6a510VgnVCM10000071d60f89RCRD&vgnextchannel=1a66e03bb8d1e310VgnVCM10000071d60f89RCRD)

## 2. Table Structure (Aakash)
- brief description
- Sunny-style flow chart

## 3. Technology (Open)
- tbd

## 4. Data Processing (Aakash)
- process overview, timing
- reference to API
- issues

## 5. Bliptrack UI (Dan)

#### Accessing Bliptrack:
- The City of Toronto's Bliptrack webservice can be accessed through the browser at `https://g4apps.bliptrack.net`
- After logging in, the default homepage is the Dashboard which is completely configurable. Some exaples of what can be displayed here are:
  - Maps of sensor locations
  - Travel time distributions for key corridors
  - Detection counts over time 
  
#### Terms:
- **Route**: A combination of any two sensors, can be configued at any time by any superuser. Most useful routes have already been created and follow either a letter or numbered convention. Once a route is created, a corresponding `routeId` is generated which can be used in the API
- **Report**: Travel time information for any route, can be configured to a number of different aggregation levels, downloaded as a `.csv` file

#### Pulling travel time data:
1. Navigate to the `Reports` window using the bar at the top of the webpage![](https://github.com/CityofToronto/bdit_data-sources/blob/master/bluetooth/blip_screenshots/report_tab.PNG)
2. Select the route you want to export data for by right clicking on it and navigating to `export data` ![](https://github.com/CityofToronto/bdit_data-sources/blob/master/bluetooth/blip_screenshots/select_route.PNG)
3. Confirgure export settings, most important parameters are:
  - Start and End date
  - Outcome columns (Bluetooth, WiFi, Both)
  - Calculation interval
4. Click `Export Data`, download will begin automatically ![](https://github.com/CityofToronto/bdit_data-sources/blob/master/bluetooth/blip_screenshots/config_report.PNG)


#### Common Issues:
- Pulling larges volumes of data can cause the server to time out, pulling more than one month of data at a time is not recommended 
- Only aggregated data is available through the browser, the API must be used to acess raw data

## 6. Bliptrack API (Dan)
Bliptrack provides an API for accessing their system through the Simple Object Access Protocol (SOAP). For those unfamiliar with SOAP, it is well explained in its [wikipedia](https://en.wikipedia.org/wiki/SOAP) article. In the context of data analysis, using the API over the browser to pull data has 2 main avantages:
- The ability to pull disagregate data
- Access to live travel time information 

The WSDL file for accessing Bliptrack can be accessed using `https://g4apps.bliptrack.net/ws/bliptrack/ExportWebServiceStateless?wsdl`

#### Pulling travel time data:

The `exportPerUserData()` method is used to pull raw data. It takes `username`, `password`, and `config` as input parameters. The `config` object contains all information required to specify the route to pull data from. Info about config is shown below

![](https://github.com/CityofToronto/bdit_data-sources/blob/master/bluetooth/blip_screenshots/config_object.PNG)

Not all of these fields must be assigned in order to pull data. At a minimum the following fields must have a non-`None` value:
- `analysisId`: `int`, designates route, more on this below
- `startTime`: `datetime` object indicating first data point
- `endTime`: `datetime` object indicating last possible data point
- `includeOutliers`: `boolean` indicating whether or not to indlude detections that Bliptrack deems to be outliers
- `live`: `boolean` indicating whether live or historic data is being pulled, should be set to `False` to pull historic data

#### The `analysisId`
Each route has a corresponding `routeId`, `reportId`, and `analysisId` - all of which are different. An `analysis` object contains all of this information. The `getExportableAnalyses()` method will return a list of all analyses available. One can then pass the `analysis.id` for any given `analysis` into `config.analysisId` when pulling data.



## 2. Table Structure (Aakash)
- brief description
- Sunny-style flow chart

### Observations 
|Column|Type|Notes|
|------|----|-----|
|id|bigserial| Primary Key |
|user_id|bigint| |
|analysis_id|integer| |
|measured_time|integer| |
|measured_time_no_filter|integer| |
|startpoint_number|smallint| |
|startpoint_name|character varying(8)| |
|endpoint_number|smallint| |
|endpoint_name|character varying(8)| |
|measured_timestamp|timestamp without time zone| |
|outlier_level|smallint| |
|cod|bigint| integer representation of 24 bit Bluetooth class |
|device_class|smallint| |

#### Filtering devices
Two fields are relevant for this endeavour, both are integer representations of binary. They are aggregations/concatenations of multiple different boolean values (bits).
 - **device_classes:** "are report/filtering dependent as they are configurable property mappings". These appear to be somewhat independent from the `cod` values below. A cursory examination of the binary forms of the two fields didn't reveal any common patterns.
 - **cod:** Is the integer representation of the [Bluetooth Class of Device property](https://www.question-defense.com/2013/01/12/bluetooth-cod-bluetooth-class-of-deviceclass-of-service-explained). It must be converted to binary since this property is an aggregation of a number of .  The main filter is that if this value is 0, the device is a WiFi device, else it's a Bluetooth device. For further filtering, have a look at the documentation linked above. 
    For example, the most common `cod` after WiFi (0) is `7995916`, its binary is `011110100000001000001100` which is exactly the example [given here](https://www.question-defense.com/2013/01/12/bluetooth-cod-bluetooth-class-of-deviceclass-of-service-explained): a smartphone. 
    
To get major and minor classes from the cod:
```sql
substring(cod::bit(24) from 17 for 6) as minor_device_class,
substring(cod::bit(24) from 12 for 5) as major_device_class
```
