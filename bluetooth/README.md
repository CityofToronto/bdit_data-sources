# Bluetooth - Bliptrack

## Table of Contents
1. [Overview](#1-overview)
2. [Table Structure](#2-table-structure)
3. [Technology](#3-technology)
4. [Data Processing](#4-data-processing)
5. [Bliptrack UI](#5-bliptrack-ui)
6. [Bliptrack API](#6-bliptrack-api)
7. [Bliptrack API OD Data](#7-bliptrack-api-od-data)

## 1. Overview (Aakash)
- coverage
- what does the data look like?
The live feed, and archived data are available on the [Open Data Portal](http://www1.toronto.ca/wps/portal/contentonly?vgnextoid=0b3abcfaf9c6a510VgnVCM10000071d60f89RCRD&vgnextchannel=1a66e03bb8d1e310VgnVCM10000071d60f89RCRD)

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
|device_class|smallint| integer representation of a bitstring `outcome` defined for that particular route |

#### Filtering devices
Two fields are relevant for this endeavour, both are integer representations of binary. They are aggregations/concatenations of multiple different boolean values (bits).
 - **device_classes:** "are report/filtering dependent as they are configurable property mappings". These appear to be somewhat independent from the `cod` values below. A cursory examination of the binary forms of the two fields didn't reveal any common patterns.
 - **cod:** Is the integer representation of the [Bluetooth Class of Device property](https://www.question-defense.com/2013/01/12/bluetooth-cod-bluetooth-class-of-deviceclass-of-service-explained). The main filter is that if this value is 0, the device is a WiFi device, else it's a Bluetooth device. There is no way of knowing what kind of device a WiFi device is. See the [`ClassOfDevice`](#classofdevice) table below for more information.

### all_analyses

The script pulls the route configurations nightly from the Blip server. These are currently being primarily dumped as json records, which makes using some of the elements of the configuration trickier in PostgreSQL

|Column|Type|Notes|
|------|----|-----|
|device_class_set_name|text|Name of the configuration for setting the `device_class` bits, see `outcomes` |
|analysis_id|bigint| One of the unique IDs for this route |
|minimum_point_completed|json| spatial configuration of the gates in the route (some have particular waypoints, or exit gates) |
|outcomes|json| lookup for `deviceClassMask` the value for `device_class` and the `name` of that particular result |
|report_id|bigint|One of the unique IDs for this route|
|report_name|text| |
|route_id|bigint|One of the unique IDs for this route|
|route_name|text| |
|route_points|json| spatial representation of the route |
|pull_data|boolean| (defaults to false) whether the script should pull observations |
`outcomes` are set for different routes for purposes like: filtering BT and WiFi, or tracking Origin Destination points. 

### ClassOfDevice

|Column|Type|Notes|
|------|----|-----|
|cod_hex|bytea| Class of device in hexidecimal|
|cod_binary|bit varying(24)| Class of device in binary|
|device_type|character varying(64)| Major-minor device class description |
|device_example|text| Example of the device|
|confirmed|character varying(10)| Observed example (Y/N) |
|confirmed_example|text| Example of the device |
|major_device_class|text| Primary type of device, e.g.: Computer, Phone, etc... |
|cod|bigint| Integer representation of the `cod_binary`, key in [`observations`](#observations) above|

The Class of Device property helps broadcast the functionality of a given Bluetooth device. It is an aggregation of a number of bits (0,1), hence having a binary representation. This comprises 3 sub-fields (see [this explanation](https://www.question-defense.com/2013/01/12/bluetooth-cod-bluetooth-class-of-deviceclass-of-service-explained)), binary strings are indexed from right to left:
 - **Device Functionality (Major Service Class, bits 13-23)**: 11 different boolean values to represent whether the device can be used for positioning, for audio, telephony, etc... 
 - **Major Device Class (Bits 8-12)**: Primary categories for the device: Miscellaneous, Computer, Phone, LAN/Network Access Point, Peripheral, Imaging, Wearable, Toy, Health, Uncategorized, and Reserved. These are present in the `major_device_class` column. 
 - **Minor Device Class (Bits 2-7)**: Further device details. These are dependent on the Major Device Class. 

[`Examples/class_of_device.ipynb`](Examples/class_of_device.ipynb) explores the distributions of these different device classes between Adelaide, an arterial, and the expressways. The most common `cod` after WiFi (0) is `7995916`, its binary is `011110100000001000001100` which is exactly the example [given here](https://www.question-defense.com/2013/01/12/bluetooth-cod-bluetooth-class-of-deviceclass-of-service-explained): a smartphone. 

According to the vendor, commonly accepted filters for cars are:
 - Car Audio: major class 00100 and minor class 001000
 - Hands Free: major class 00100 and minor class 000100

To get major and minor classes from the cod:
```sql
substring(cod::bit(24) from 17 for 6) as minor_device_class,
substring(cod::bit(24) from 12 for 5) as major_device_class
```

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
- **Route**: A combination of any two sensors, can be configured at any time by any superuser. Most useful routes have already been created and follow either a letter or numbered convention. Once a route is created, a corresponding `routeId` is generated which can be used in the API
- **Report**: Travel time information for any route, can be configured to a number of different aggregation levels, downloaded as a `.csv` file

#### Pulling travel time data:
1. Navigate to the `Reports` window using the bar at the top of the webpage![](https://github.com/CityofToronto/bdit_data-sources/blob/master/bluetooth/blip_screenshots/report_tab.PNG)
2. Select the route you want to export data for by right clicking on it and navigating to `export data` ![](https://github.com/CityofToronto/bdit_data-sources/blob/master/bluetooth/blip_screenshots/select_route.PNG)
3. Configure export settings, most important parameters are:
  - Start and End date
  - Outcome columns (Bluetooth, WiFi, Both)
  - Calculation interval
4. Click `Export Data`, download will begin automatically ![](https://github.com/CityofToronto/bdit_data-sources/blob/master/bluetooth/blip_screenshots/config_report.PNG)


#### Common Issues:
- Pulling larges volumes of data can cause the server to time out, pulling more than one month of data at a time is not recommended 
- Only aggregated data is available through the browser, the API must be used to acess raw data

## 6. Bliptrack API (Dan)
Bliptrack provides an API for accessing their system through the Simple Object Access Protocol (SOAP). For those unfamiliar with SOAP, it is well explained in its [wikipedia](https://en.wikipedia.org/wiki/SOAP) article. In the context of data analysis, using the API over the browser to pull data has 2 main advantages:
- The ability to pull disaggregate data
- Access to live travel time information 

The WSDL file for accessing Bliptrack can be accessed using `https://g4apps.bliptrack.net/ws/bliptrack/ExportWebServiceStateless?wsdl`

#### Pulling travel time data:

The `exportPerUserData()` method is used to pull raw data. It takes `username`, `password`, and `config` as input parameters. The `config` object contains all information required to specify the route to pull data from. Info about config is shown below

![](https://github.com/CityofToronto/bdit_data-sources/blob/master/bluetooth/blip_screenshots/config_object.PNG)

Not all of these fields must be assigned in order to pull data. At a minimum the following fields must have a non-`None` value:
- `analysisId`: `int`, designates route, more on this below
- `startTime`: `datetime` object indicating first data point
- `endTime`: `datetime` object indicating last possible data point
- `includeOutliers`: `boolean` indicating whether or not to include detections that Bliptrack deems to be outliers
- `live`: `boolean` indicating whether live or historic data is being pulled, should be set to `False` to pull historic data

#### The `analysisId`
Each route has a corresponding `routeId`, `reportId`, and `analysisId` - all of which are different. An `analysis` object contains all of this information. The `getExportableAnalyses()` method will return a list of all analyses available. One can then pass the `analysis.id` for any given `analysis` into `config.analysisId` when pulling data.


## 7. Bliptrack API OD Data
The API pulls a lot of data from BlipTrack, some of which is Origin-Destination (OD) data. OD describes the first and last sensors a device is seen at (Start-End Data), as well as the in-between sensors the device is seen at (Others Data).

### Start-End Data
The Start-End Data describes the trips of each device, with each device being identified with a `userId`. Attributes of interest are `measuredTime`, `measuredTimestamp`, `outlierLevel`, `cod`, `deviceClass`. 

*Some notes on `measuredTime` and records:*
If a device moves within range of a sensor after having not been seen by a sensor for over 20 minutes, a new record with the same UserID but a different timestamp will be created. If a device has been detected by a sensor for over a period of 90 minutes, its record will be cut off at the 90 minute mark and a new record will not be created for that device until a different sensor picks it up. Based on these two conditions, a device can have multiple records in the data. 

Each record is a dictionary, which follows the structure and format as seen in the table below.

#### Dictionary Structure
|Attribute|Type|Notes|
|---------|----|-----|
|userId|bigint| |
|analysisId|integer| |
|measuredTime|integer|Duration (seconds) between when the device was first seen and last seen|
|measuredTimeNoFilter|integer| |
|startPointNumber|smallint| |
|startPointName|string| |
|endPointNumber|smallint| |
|endPointName|string| |
|measuredTimestamp|timestamp string|timestamp of when the device was last detected|
|routeStartTimestamp|string|None|
|outlierLevel|smallint| |
|cod|bigint| integer representation of Bluetooth class |
|deviceClass|numeric|stores bit representation of sensors that the device passed|
|outcome_match|array|empty|

### Others Data
The Others Data is differentiated from the Start-End Data by the `analysisId` that is called, and the data itself differs by the `deviceClass` and the `outlierLevel`. It follows the same structure and format as the Start-End Data. 

### deviceClass and outlierLevel
`deviceClass` is a numerical value with 17 to 33 digits. This number, when converted into a bitstring, reflects the sensors ("gates") that the device `userId` has been seen at. The positions of the bits that are set `1` directly correspond to gate values, which are tied to human-friendly descriptions. 

Gate values are powers of 2, based on the order of the gates in the lookup list, starting from 0 (e.g. 5th gate value = 2^(5-1)). The value in `deviceClass` is the sum of gates' values that the device has passed. To know which gates were passed, the `deviceClass` can be converted into bits, and the positions of the set bits in the bitstring are also the position of the gates in their lookup list. 

To get the gate values from the `deviceClass` value:
1. Convert the `deviceClass` value into bits
2. Reverse the order of the bits
3. Iterate through the bitstring to find the positions of the set bits
4. Take the position of each set bit `p` and calculate the gate value `g`
	* `g = 2^p`

#### For the Start-End Data:
`deviceClass` is expected to return 2 values, but can return up to 3: the first value being the start gate, and the second value being the end gate. If there is a third value, it means that the device was detected at only one sensor; the expected two values will point to the same sensor but with different numbers, and the third value will be `324518553658426726783156020576256`, representing `OneSensorOnly`.

#### For the Others Data:
`deviceClass` is expected to return at least 2 values, with the last value always representing if the device was `HandsFree` or `Non-HandsFree`. The other values it returns are the gates that the device was seen at, in the order of the lookup list. With OD Data alone we cannot tell the order of gates a device is seen by. 

If the values of a Others `deviceClass` are calculated, sharing the same `UserId` and timestamp as a Start-End `deviceClass` that was `OneSensorOnly`, then the Others `deviceClass` will only return 2 values, the first being the value of the one sensor that detected the device, and the second still being the value indicating if the device was or was not a handsfree device.

#### outliersLevel
Looking at the `outliersLevel` of the Start-End Data is a quick way to check if a device only passed by one sensor. If the `deviceClass` has the `OneSensorOnly` value, the `outlierLevel` attribute will have a value of `3`. The `outlierLevel` of the Others Data of the same record (`userId` and timestamp) will be 0, however. 
