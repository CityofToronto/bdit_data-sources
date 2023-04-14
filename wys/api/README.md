# Watch Your Speed Sign API

## Overview

!['A sign mounted on a pole with the words "Your Speed" and underneath a digital sign displaying "31"'](https://www.toronto.ca/wp-content/uploads/2018/09/9878-landscape-mwysp2-e1538064432616-1024x338.jpg)

The city has installed [Watch Your Speed Signs](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-initiatives/initiatives/watch-your-speed-program/) that display the speed a vehicle is travelling at and flashes if the vehicle is travelling over the speed limit. Installation of the sign was done as part of 3 programs: the normal watch your speed sign program, mobile watch your speed which has signs mounted on trailers that move to a different location every few weeks, and school watch your speed which has signs installed at high priority schools. As part of the [Vision Zero Road Safety Plan](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/), these signs aim to reduce speeding.

This API script can grab data from each watch your speed sign the city has. It can perform some functionality that the [streetsoncloud portal](www.streetsoncloud.com) has, mainly pulling speed and volume data. The API supports more calls including setting/getting the schedule, setting/getting the messages each sign displays and other calls.

## Functionality

### Requesting recent data

The script can request data on any day specified by the API call.

### Error Handling

Certain errors, e.g., `requests` or `504` errors, will not cause the script to exit. The script will sleep a pre-determined amount of time, and then retry the API call for specific number of retries. Any missing data is reported in Airflow logs.

The number of signs sending data is not a constant number and changes every day. The script has a check that finds out the number of signs that are reporting valid data, and will enter any signs that started to report to the `locations` table.

### Inconsistent time bins

The API provides data on non-regular (but uniform) 5 minute bins, i.e., at 3:44, 3:49, etc. This change has been accomodated in the tables, and for the 15 minute aggregation tables.

## Calls and Input Parameters

The WYS data puller script is called by an Airflow DAG that runs daily. It collects the count of vehicles for each speed recorded by the sign in roughly 5 minute aggregate bins.

The WYS data puller script can also run independent of Airflow for specific date ranges and locations. It uses the `click` module like the `miovision` and `here` data to define input paramters. The argument to run the API is `run_api`, e.g., `python wys_api.py run_api`. It .

|Option|Type|Format|Description|Example|Default|
|-----|------|------|-------|-----|-----|
|`start_date`|str|`"YYYY-MM-DD"`|The start date of pulled data|`"2023-01-31"`|previous day date|
|`end_date`|str|`"YYYY-MM-DD"`|The end date of pulled data|`"2023-01-31"`|previous day date|
|`path`|str||The path of the configuration file|`/home/wys/api/config.cfg`|`config.cfg` (int the current directory)|
|`location_flag`|integer||The location ID used by the API to pull data from a specific intersection|`1967`|`0` (to pull all available data)|

## Process

The main function in the puller script `api_main` do the following steps:

1. Parse the `config.cfg` file (for API key and database credentials) and any input parameters.
2. Retrieve the list of all signs by calling `location_id()` function, if `location_flag` is `0`.
3. For every day in the parsed date range (defined as `[start_date, end_date]`):
   1. Collect daily data by calling `get_data_for_date()`, which attempts to pull hourly statistics for each location up to three trials before marking this (sign, hour) pair as failed.
   2. Insert the collected data into the appropriate `raw_data` table, e.g., `wys.raw_data_2023` for 2023 data
   3. Aggregate speed counts by calling the `plpgsql` function `wys.aggregate_speed_counts_one_hour_5kph()`
4. Update the `wys.locations` table with the changes in the direction and/or location (moved for more than 100 meters) of any existing sign and insert the locations of the new signs as well.
5. Update the `wys.sign_schedules_list` table, if needed

## PostgreSQL Processing

### Data Tables

The data is inserted into `wys.raw_data`. Data from the API is already pre-aggregated into roughly 5 minute bins.

|Field name|Data type|Description|Example|
|------|------|-------|------|
`raw_data_uid`|integer|A unique identifier for the `raw_data` table|2655075
`api_id`|integer|ID used for the API, and unique for individual signs, which could be moved between different locations, i.e., some `api_id`s could exist more than once in the `locations` table|1967
`datetime_bin`|timestamp|Start time of the bin|2018-10-29 10:00:00
`speed`|integer|Exact speed of the number of vehicles in `count`|47
`count`|integer|Number of vehicles in datetime_bin/api_id/speed combination|2
`speed_count_uid`|integer|A unique identifier for `speed_counts_agg_5kph` table. Indicates if the data has already been processed or not.|150102

`wys.speed_counts_agg_5kph` has data aggregated to 1-hour and 5 km/h bins using the `aggregate_speed_counts_one_hour_5kph()` function. Values for the speed bins are replaced by lookup table IDs.

|Field name|Data type|Description|Example|
|------|------|-------|------|
`speed_counts_agg_5kph_id`|integer|A unique identifier for the `speed_counts_agg_5kph` table|2655075
`api_id`|integer|ID used for the API, and unique identifier for the `locations` table|1967
`datetime_bin`|timestamp|Start time of the 1 houraggregated bin|2018-10-29 10:00:00
`speed_id`|integer|A unique identifier for the 5 kph speed bin in the `speed_bins` table|5
`count`|integer|Number of vehicles in datetime_bin/api_id/speed bin combination|7

### Lookup Tables

`speed_bins` is a lookup table containing all the 5km/h speed bin. Bin number 25 contains any speed over 120km/h.

|Field name|Data type|Description|Example|
|------|------|-------|------|
`speed_id`|integer|A unique identifier for the `speed_bins` table|5
`speed_bin`|integer range|Range of speeds for each speed bin. The upper limit is not inclusive.|[10-25)

`locations` contains each ID used for the API, direction of traffic the sign is gathering data for, address and name of the sign. Some signs may have information missing from any one of the fields.

|Field name|Data type|Description|Example|
|------|------|-------|------|
`api_id`|integer|ID used for the API, and unique identifier for the `locations` table|1967
`address`|text|Address of the sign|1577 Bloor Street West
`sign_name`|text|Name of the sign. May include address + serial number, the ward name for the Mobile WYSP, or school name for Schools WYSP|Dundas St W SB 16101191
`dir`|text|Direction of the flow of traffic|NB

### Views

`report_dates` contains the list of days, for each sign, where there is more than 10 hours of data between 6:00 and 22:00.

|Field name|Data type|Description|Example|
|------|------|-------|------|
`api_id`|integer|ID used for the API, and unique identifier for the `locations` table|1967
`period`|text|Month the data is in|Nov 2018
`dt`|date|Date where there is more than 10 hours of data|2018-11-03
`dow`|integer|Day of the week. Sunday is 0, and Saturday is 6|4

## Quality Checks

### NULL rows in API data

An analysis on `2021-04-23` to investigate rows with NULL speed and count columns was performed after noticing that NULL rows had started to appear in the database on `2021-03-31`. For details see notebook `investigate_api_nulls.ipynb` in this directory , as part of [issue #393](https://github.com/CityofToronto/bdit_data-sources/issues/393). Main findings:

- there are currently `872` distinct signs in `wys.raw_data`  
- `727` of these contain rows with NULL speed or count columns  
- `739` signs have been operating since `> 2021-03-31`
- of the `734` signs have operating since `> 2021-03-31`, `734` have NULL speed or count columns

Please see the notebook for a Gantt-style visualization of the NULL date ranges. 
