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
- Route: A combination of any two sensors, can be configued at any time by any superuser. Most useful routes have already been created and follow either a letter or numbered convention. Once a route is created, a corresponding `routeId` is generated which can be used in the API
- Report: Travel time information for any route, can be configured to a number of different aggregation levels, downloaded as a `.csv` file

#### Pulling travel time data:
1. Navigate to the `Reports` window using the bar at the top of the webpage![](https://github.com/CityofToronto/bdit_data-sources/blob/master/bluetooth/blip_screenshots/report_tab.PNG)
2. Select the route you want to export data for by right clicking on it and navigating to `export data` ![](https://github.com/CityofToronto/bdit_data-sources/blob/master/bluetooth/blip_screenshots/select_route.PNG)
3. Confirgure export settings, most important parameters are:
  - Start and End date
  - Outcome columns (Bluetooth, WiFi, Both)
  - Calculation interval
4. Click `Export Data`, download will begin automatically ![](https://github.com/CityofToronto/bdit_data-sources/blob/master/bluetooth/blip_screenshots/config_report.PNG)
- how to access Bliptrack dashboard
- how to pull data
- glossary (i.e. what is a route?, what is a report?, etc.)

#### Common Issues:

## 6. Bliptrack API (Dan)
Bliptrack provides an API for accessing their system through the Simple Object Access Protocol (SOAP). 
- overview of API methods


