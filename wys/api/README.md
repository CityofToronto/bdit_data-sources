# Watch Your Speed Sign API

## Overview

!['A sign mounted on a pole with the words "Your Speed" and underneath a digital sign displaying "31"'](https://www.toronto.ca/wp-content/uploads/2018/09/9878-landscape-mwysp2-e1538064432616-1024x338.jpg)

The city has installed [Watch Your Speed Signs](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-initiatives/initiatives/watch-your-speed-program/) that display the speed a vehicle is travelling at and flashes if the vehicle is travelling over the speed limit. Installation of the sign was done as part of 3 programs: the normal watch your speed sign program, mobile watch your speed which has signs mounted on trailers that move to a different location every few weeks, and school watch your speed which has signs installed at high priority schools. As part of the [Vision Zero Road Safety Plan](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/), these signs aim to reduce speeding.

This API script can grab data from each watch your speed sign the city has. It can perform some functionality that the [streetsoncloud portal](www.streetsoncloud.com) has, mainly pulling speed and volume data. The API supports more calls including setting/getting the schedule, setting/getting the messages each sign displays and other calls.

## Functionality

### Requesting recent data

The script can request data on any day specified by the API call.

### Error Handling

Certain errors like a `requests` error or a `504` error will not cause the script to exit. The script will sleep a pre-determined amount of time, and then retry the API call. 

The number of signs sending data is not a set number and changes every day. The script has a check that finds out the number of signs that are reporting valid data, and will enter any signs that started to report to the `locations` table.

### Inconsistent time bins

The API only gives data on non-regular (but uniform) 5 minute bins, meaning data is given at 3:44, 3:49 etc. This change has been accomodated in the tables, and for the 15 minute aggregation tables.

## Calls and Input Parameters

The script uses the `get statistics` call to get the volume and speed data. It will return the count of vehicles for each speed recorded by the sign in roughly 5 minute aggregate bins.

The script uses the `click` module like the `miovision` and `here` data to define input paramters. The argument to run the API is `run_api`, for example: `python wys_api.py run_api` will start the script without any input parameters.

|Option|Format|Description|Example|Default|
|-----|------|-------|-----|-----|
`--minutes`|integer|The amount of minutes to pull data for|`30`|`1473`
|`--pull_time`|`HH:mm`|The time when the script will pull data. Since the API does not support pulling data for specific datetimes at the moment, this should be coordinated with `--minutes` to ensure the right amount of data is pulled. It is recommended this time be at least 3 minutes in the future if this is specified.|`15:25`|`0:01`
`--path`|directory|Specifies the directory where the `config.cfg` file is|`C:\Users\rliu4\Documents\GitHub\bdit_data-sources\wys\api`|`config.cfg` (same directory as python script)
`--location_flag`|integer|The location ID used by the API to pull data from a specific intersection|`1967`|`0` (Will pull all the data at all intersecitons available)

## Process

1. The script parses the `config.cfg` file and any input parameters.
2. The script will retrieve the list of all signs by using the `signs` call in `get_signs()` function.
3. The script will check all the signs if they have data by using the `statistics` call in `get_location()` and returning a string of the output. If theres no data, possible outputs may be a `403` error or an `HTML` code snippet. The script will create a table of all valid locations.
4. The script will again call the `statistics` call in `get_statistics()` to grab the counts and speed.
5. The script will insert the data to postgreSQL.
6. The script will check to see if there are new `api_id` not in `wys.locations`. If so, it will retrieve information about these signs using `get_location`, parse the data into a suitable format, and insert it to `wys.locations`.
7. The script will call the 2 aggregation functions in postgreSQL and refresh the views.

## PostgreSQL Processing

### Data Tables

The data is inserted into `wys.raw_data`. Data from the API is already pre-aggregated into roughly 5 minute bins.

|Field name|Data type|Description|Example|
|------|------|-------|------|
`raw_data_uid`|integer|A unique identifier for the `raw_data` table|2655075
`api_id`|integer|ID used for the API, and unique identifier for the `locations` table|1967
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
