# Watch Your Speed Sign API

## Overview

!['A sign mounted on a pole with the words "Your Speed" and underneath a digital sign displaying "31"'](https://www.toronto.ca/wp-content/uploads/2018/09/9878-landscape-mwysp2-e1538064432616-1024x338.jpg)

The city has installed [Watch Your Speed Signs](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-initiatives/initiatives/watch-your-speed-program/) that display the speed a vehicle is travelling at and flash if the vehicle is travelling over the speed limit. Installation of the sign was done as part of 2 [WYS programs](../readme.md): stationary watch your speed signs (near schools) and mobile watch your speed signs mounted on trailers that move to a different location every few weeks (mostly every three weeks with a few longer exceptions). As part of the [Vision Zero Road Safety Plan](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/), these signs aim to reduce speeding.

This API script pulls WYS speed and volume data from [streetsoncloud portal](http://www.streetsoncloud.com) using their API. This API supports more calls including setting/getting the schedule, setting/getting the messages each sign displays and other calls.

## Functionality

### Requesting recent data

The script can request data on any day specified by the API call.

### Error Handling

Certain errors, e.g., `requests` or `504` errors, will not cause the script to exit. The script will sleep a pre-determined amount of time, and then retry the API call for specific number of retries. Any missing data is reported in Airflow logs.

The number of signs sending data is not a constant number and changes every day. The script has a check that finds out the number of signs that are reporting valid data, and will enter any signs that started to report to the `locations` table.

### Inconsistent time bins

The API provides data on non-regular (but uniform) 5 minute bins, i.e., at 3:44, 3:49, etc. This change has been accomodated in the tables, and for the 15 minute aggregation tables.

## Calls and Input Parameters

The WYS data puller script is called by an Airflow DAG that runs daily. It collects the count of vehicles for each speed recorded by the sign in 5 minute aggregate bins.

The WYS data puller script can also run independent of Airflow for specific date ranges and locations. It uses the `click` module like the `miovision` and `here` data to define input paramters. The argument to run the API is `run_api`, e.g., `python wys_api.py run_api`.

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

**`wys.raw_data`**  
- This table is partitioned by year: `wys.raw_data_20**`
- The data in `wys.raw_data` table is pre-aggregated into 5 minute bins, and the table has the following columns:

|Field name|Type|Description|Example|
|------|------|-------|------|
|`api_id`|integer|ID used for the API, and unique for individual signs, which could be moved between different locations (for mobile signs), i.e., some `api_id`s could exist more than once in the `locations` table|1967|
|`datetime_bin`|timestamp|Start time of the bin|2018-10-29 10:00:00|
|`speed`|integer|Exact speed of the number of vehicles in `count`|47|
|`count`|integer|Number of vehicles in (datetime_bin,api_id,speed) combination|2|
|`speed_count_uid`|integer|A unique identifier for `speed_counts_agg_5kph` table. Indicates if the data has already been processed or not.|150102|

**`wys.speed_counts_agg_5kph`**  
As described above, `wys.speed_counts_agg_5kph` has data aggregated to 1-hour and 5 km/h bins using the `aggregate_speed_counts_one_hour_5kph()` function. Values for the speed bins are replaced by lookup table IDs.

|Field name|Type|Description|Example|
|------|------|-------|------|
|`speed_counts_agg_5kph_id`|bigint|A unique identifier for the `speed_counts_agg_5kph` table|2655075|
|`api_id`|integer|ID used for the API, and unique identifier for the `locations` table|1967|
|`datetime_bin`|timestamp|Start time of the 1 houraggregated bin|2018-10-29 10:00:00|
|`speed_id`|integer|A unique identifier for the 5 kph speed bin in the `speed_bins` table|5|
|`volume`|integer|Number of vehicles in datetime_bin/api_id/speed bin combination|7|

### Lookup Tables

**`wys.speed_bins_old`**  
This is a lookup table containing all the 5km/h speed bin, where bin number 21 contains any speed over 100km/h.

|Field name|Type|Description|Example|
|------|------|-------|------|
`speed_id`|integer|A unique identifier for the `speed_bins` table|5
`speed_bin`|integer range|Range of speeds for each speed bin. The upper limit is not inclusive.|[10-15)

**`wys.locations`**  
This table contains locations of stationary and mobile signs. It also contains information about each sign's API ID, direction of traffic, address, and name.

|Field name|Type|Description|Example|
|------|------|-------|------|
|`api_id`|integer|ID used for the API, and unique for each sign (most mobile signs change locations every three weeks)|1967|
|`address`|text|Address of the sign|1577 Bloor Street West|
|`sign_name`|text|Name of the sign. May include address + serial number, the ward name for the Mobile WYSP, or school name for Schools WYSP|Dundas St W SB 16101191|
|`dir`|text|Direction of the flow of traffic|NB|
|`start_date`|date|First date of valid data|2018-11-28|
|`loc`|text|The coordinates of the sign|(43.666115,-79.370164)
|`id`|integer|Unique ID and primary key of the table. This is the same as `sign_id` in `wys.stationary_signs`.|1|
|`geom`|geometry|The location of the sign calculated from `loc`||

**`wys.ward_masterlist`**  
This table contains a list of the Google Sheets containing the the Mobile WYS sign locations.

| column_name            | data_type   | sample                   | Comments   |
|:-----------------------|:------------|:-------------------------|:-----------|
| ward_no                | integer     | 1                        |            |
| ward_name              | text        | Etobicoke North          |            |
| community_council_area | text        | Etobicoke-York           |            |
| councillor_2018_2022   | text        | Michael Ford             |            |
| links                  | text        | (REMOVED FOR SECURITY)   |            | 
| spreadsheet_id         | text        | (REMOVED FOR SECURITY)   |            |
| range_name             | text        | A10:N999                 |            |
| schema_name            | text        | wys                      |            |
| table_name             | text        | ward_1                   |            |
| contact                | text        |                          |            |
| contact_2              | text        |                          |            |

**`wys.mobile_sign_installations`**  
This table contains the details of the mobile sign installations taken from the Google Sheets listed in `wys.ward_masterlist`. 
- This table is partitioned by ward (`wys.ward_*`). 
- New signs are inserted daily from the Google Sheets, as long as they have a valid `installation_date` and `new_sign_number`. 
- If there is a discrepency between the information online and an existing row in this `mobile_sign_installations`, the row is updated and the change is logged in `wys.logged_actions`. 
- If there are duplicates pulled from the Google Sheets, they are instead inserted into `wys.mobile_sign_installations_dupes`, as a row cannot be updated twice by `ON CONFLICT... DO UPDATE`.

| column_name       | data_type   | sample           | Comments   |
|:------------------|:------------|:-----------------|:-----------|
| work_order        | integer     | 11869670         |            |
| ward_no           | integer     | 2                |            |
| location          | text        | Wimbleton Rd     |            |
| from_street       | text        | Anglesey Blvd    |            |
| to_street         | text        | The Kingsway     |            |
| direction         | text        | NB               |            |
| installation_date | date        | 2023-10-20       |            |
| removal_date      | date        |                  |            |
| new_sign_number   | text        | 4                |            |
| comments          | text        | 149 Wimbleton Rd |            |
| id                | integer     | 10483482         |            |

**`wys.sign_schedules_list`**  
lists the "schedule" or operating mode of each sign (api_id). It is updated daily by the `pull_wys` task `pull_schedules`.
| column_name   | data_type         | sample                                    | Comments   |
|:--------------|:------------------|:------------------------------------------|:-----------|
| schedule_name | character varying | School WYSP 30kmh Reg. Operating Schedule |            |
| api_id        | integer           | 10294                                     |            |

`wys.sign_schedules_clean` contains details about each of the `schedule_name`s listed in `wys.sign_schedules_list`. It is not regularly updated. 
| column_name   | data_type         | sample                              | Comments   |
|:--------------|:------------------|:------------------------------------|:-----------|
| schedule_name | character varying | Beacon Test 40KM School             |            |
| sign_type     | character varying | SP500/550 - Static SPEED LIMIT Sign |            |
| schedule      | text              | Weekdays from 7 AM - 9 PM           |            |
| min_speed     | integer           | 30                                  |            |
| speed_limit   | integer           | 40                                  |            |
| flash_speed   | integer           | 45                                  |            |
| strobe_speed  | integer           | 30                                  |            |

## Quality Checks

### NULL rows in API data

An analysis on `2021-04-23` to investigate rows with NULL speed and count columns was performed after noticing that NULL rows had started to appear in the database on `2021-03-31`. For details see notebook [investigate_api_nulls.ipynb](./investigate_api_nulls.ipynb), as part of [issue #393](https://github.com/CityofToronto/bdit_data-sources/issues/393). Main findings:

- there are currently `872` distinct signs in `wys.raw_data`  
- `727` of these contain rows with NULL speed or count columns  
- `739` signs have been operating since `> 2021-03-31`
- of the `734` signs have operating since `> 2021-03-31`, `734` have NULL speed or count columns

Please see the notebook for a Gantt-style visualization of the NULL date ranges. 

## Mobile Signs

Sign locations are identified using Google Sheets listed in `wys.ward_master_list` table. 
The signs have locations listed in `wys.locations`, however these are not deemed to be trustworthy for mobile signs as they get moved around frequently (every ~3 weeks) and the locations are not updated correspondingly. 

### Summary Tables

**`wys.stationary_summary`**
- This table contains a monthly summary for each stationary WYS sign. It is used to create the Open Data view `open_data.wys_stationary_summary`. 

| column_name       | data_type   | sample     | Comments   |
|:------------------|:------------|:-----------|:-----------|
| sign_id           | integer     | 3          |            |
| mon               | date        | 2020-04-01 |            |
| pct_05            | integer     | 8          |            |
| pct_10            | integer     | 10         |            |
...............pct_15 to pct_90................
| pct_95            | integer     | 39         |            |
| spd_00            | integer     | 0          |            |
| spd_05            | integer     | 5238       |            |
| spd_10            | integer     | 5853       |            |
...............spd_15 to spd_90................
| spd_95            | integer     | 0          |            |
| spd_100_and_above | integer     | 0          |            |
| volume            | integer     | 52467      |            |
| api_id            | integer     | 7635       |            |

wys.sign_schedules
| column_name      | data_type         | sample                              | Comments   |
|:-----------------|:------------------|:------------------------------------|:-----------|
| schedule_name    | character varying | Beacon Test 40KM School             |            |
| sign_type        | character varying | SP500/550 - Static SPEED LIMIT Sign |            |
| #                | integer           | 1                                   |            |
| sun              | character varying | OFF                                 |            |
| mon              | character varying | Activated at 21:00                  |            |
| tues             | character varying | Activated at 21:00                  |            |
| wed              | character varying | Activated at 21:00                  |            |
| thurs            | character varying | Activated at 21:00                  |            |
| fri              | character varying | Activated at 21:00                  |            |
| sat              | character varying | OFF                                 |            |
| min(km/h)        | integer           | 30                                  |            |
| display_min      | text              | SPEED LIMIT                         |            |
| limit(km/h)      | integer           | 40                                  |            |
| display_limit    | text              | SPEED LIMIT                         |            |
| allowed(km/h)    | integer           | 50                                  |            |
| display_allowed  | text              | SPEED LIMIT                         |            |
| max(km/h)        | integer           | 75                                  |            |
| display_max      | text              | OFF                                 |            |
| red(km/h)        | text              |                                     |            |
| display_red      | text              |                                     |            |
| flashing_digits  | character varying | 45 km/h                             |            |
| flashing_strobe  | character varying | OFF                                 |            |
| flashing_message | character varying | OFF                                 |            |


wys.mobile_summary
| column_name       | data_type   | sample                    | Comments   |
|:------------------|:------------|:--------------------------|:-----------|
| days_with_data    | integer     | 22                        |            |
| max_date          | date        | 2023-03-21                |            |
| location_id       | integer     | 7895106                   |            |
| ward_no           | integer     | 14                        |            |
| location          | text        | Donlands Ave              |            |
| from_street       | text        | Danforth Ave              |            |
| to_street         | text        | Milverton Blvd            |            |
| direction         | text        | SB                        |            |
| installation_date | date        | 2023-02-28                |            |
| removal_date      | date        | 2023-03-22                |            |
| schedule          | text        | Weekdays from 7 AM - 9 PM |            |
| min_speed         | integer     | 20                        |            |
| pct_05            | integer     | 0                         |            |
| pct_10            | integer     | 0                         |            |
| pct_15            | integer     | 0                         |            |
| pct_20            | integer     | 0                         |            |
| pct_25            | integer     | 0                         |            |
| pct_30            | integer     | 0                         |            |
| pct_35            | integer     | 0                         |            |
| pct_40            | integer     | 0                         |            |
| pct_45            | integer     | 0                         |            |
| pct_50            | integer     | 0                         |            |
| pct_55            | integer     | 0                         |            |
| pct_60            | integer     | 0                         |            |
| pct_65            | integer     | 0                         |            |
| pct_70            | integer     | 0                         |            |
| pct_75            | integer     | 0                         |            |
| pct_80            | integer     | 0                         |            |
| pct_85            | integer     | 0                         |            |
| pct_90            | integer     | 0                         |            |
| pct_95            | integer     | 0                         |            |
| spd_00            | integer     |                           |            |
| spd_05            | integer     |                           |            |
| spd_10            | integer     |                           |            |
| spd_15            | integer     |                           |            |
| spd_20            | integer     |                           |            |
| spd_25            | integer     |                           |            |
| spd_30            | integer     |                           |            |
| spd_35            | integer     |                           |            |
| spd_40            | integer     |                           |            |
| spd_45            | integer     |                           |            |
| spd_50            | integer     |                           |            |
| spd_55            | integer     |                           |            |
| spd_60            | integer     |                           |            |
| spd_65            | integer     |                           |            |
| spd_70            | integer     |                           |            |
| spd_75            | integer     |                           |            |
| spd_80            | integer     |                           |            |
| spd_85            | integer     |                           |            |
| spd_90            | integer     |                           |            |
| spd_95            | integer     |                           |            |
| spd_100_and_above | integer     |                           |            |
| volume            | integer     |                           |            |