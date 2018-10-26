# Watch Your Speed Sign API

## Overview

The city has installed watch your speed signs that display the speed a vehicle is travelling at and flashes if the vehicle is travelling over the speed limit. Installation of the sign was done as part of 3 programs: the normal watch your speed sign program, mobile watch your speed which has signs mounted on trailers that move to a different location every few weeks, and school watch your speed which has signs installed at high priority schools. As part of the Vision Zero Road Safety Plan, these signs aim to reduce speeding.

This API script can grab data from each watch your speed sign the city has. It can perform some functionality that the [streetsoncloud portal](www.streetsoncloud.com) has, mainly pulling speed and volume data. The API supports more calls including setting/getting the schedule, setting/getting the messages each sign displays and other calls. 

## Functionality

The script has email notification and will send 1-2 emails in the event of an error; 1 for a postgreSQL related error and 1 for any error during data pulling. Multiple errors will be concatenated into 1-2 emails.

Due to the limited functionality of the API, we can only request `xx` number of minutes from when the call is made. After getting a list of valid locations, it will sleep until the specified `pull_time` before making the call for data. This is so the amount of data received is controlled. 

Similarly, the script can request a maximum of 24 hours of data. The API may be able handle requests larger than 24 hours, but the script has checks to only insert a maximum of 24 hours of data, ending at the specified `pull_time`. 

Certain errors like a `requests` error or a `504` error will not cause the script to exit. The script will sleep a pre-determined amount of time, and then retry the API call. 

The number of signs sending data is not a set number and changes every day. The script has a check that finds out the number of signs that are reporting valid data.

Since the aggregation bins reported by the API are not consistently 5 minutes long, and the start time of the bins is not consistently at an interval of 5 (ex 6:05, 6:10 etc), the API rounds each datetime to the nearest 5 minute interval.

## Calls and Input Parameters

The script uses the `get statistics` call to get the volume and speed data. It will return the count of vehicles for each speed recorded by the sign in roughly 5 minute aggregate bins.

The script uses the `click` module like the `miovision` and `here` data to define input paramters. The argument to run the API is `run_api`, for example: `python wys_api.py run_api` will start the script without any input parameters.

|Option|Format|Description|Example|Default|
|-----|------|-------|-----|-----|
`--minutes`|integer|The amount of minutes to pull data for|`30`|`1473`
|`--pull_time`|`HH:mm`|The time when the script will pull data. Since the API does not support pulling data for specific datetimes at the moment, this should be coordinated with `--minutes` to ensure the right amount of data is pulled.|`15:25`|`0:01`
`--path`|directory|Specifies the directory where the `config.cfg` file is|`C:\Users\rliu4\Documents\GitHub\bdit_data-sources\wys\api`|`config.cfg` (same directory as python script)
`--location_flag`|integer|The location ID used by the API to pull data from a specific intersection|`1967`|`0` (Will pull all the data at all intersecitons available)

## Process

1. The script parses the `config.cfg` file and any input parameters.
2. The script will retrieve the list of all signs by using the `signs` call in `get_signs()` function.
3. The script will check all the signs if they have data by using the `statistics` call in `get_location()` and returning a string of the output. If theres no data, possible outputs may be a `403` error or an `HTML` code snippet. The script will create a table of all valid locations.
4. The script will again call the `statistics` call in `get_statistics()` to grab the counts and speed.
5. The script will insert the data to postgreSQL.

## PostgreSQL Processing

The data is inserted into `wys.raw_data`. `wys.counts_15min` has aggregated 15 minute time bins, and aggregated 5 km/h speed bins by using the `aggregate_speed_counts_15min()` function. `aggregate_volumes_15min()` aggregates all the speed bins together so that in `wys.volumes_15min()` only has volume, datetime and `api_id` data.


