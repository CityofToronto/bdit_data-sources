## Watch Your Speed

!['A sign mounted on a pole with the words "Your Speed" and underneath a digital sign displaying "31"'](https://www.toronto.ca/wp-content/uploads/2018/09/9878-landscape-mwysp2-e1538064432616-1024x338.jpg)

The city has installed [Watch Your Speed Signs](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/safety-initiatives/initiatives/watch-your-speed-program/) that display the speed a vehicle is travelling at and flashes if the vehicle is travelling over the speed limit. Installation of the sign was done as part of 3 programs: the normal watch your speed sign program, mobile watch your speed which has signs mounted on trailers that move to a different location every few weeks, and school watch your speed which has signs installed at high priority schools. As part of the [Vision Zero Road Safety Plan](https://www.toronto.ca/services-payments/streets-parking-transportation/road-safety/vision-zero/), these signs aim to reduce speeding.

## Open Data

Semi-aggregated and monthly summary data are available for the two programs (Stationary School Safety Zone signs and Mobile Signs) and are updated monthly. Because the mobile signs are moved frequently, they do not have accurate locations beyond a text description, and are therefore presented as a separate dataset. See [WYS documentation](api/README.md) for more information on how the datasets are processed.

  - [School Safety Zone Watch Your Speed Program – Locations](https://open.toronto.ca/dataset/school-safety-zone-watch-your-speed-program-locations/): The locations and operating parameters for each location where a permanent Watch Your Speed Program Sign was installed.
  - [School Safety Zone Watch Your Speed Program – Detailed Speed Counts](https://open.toronto.ca/dataset/school-safety-zone-watch-your-speed-program-detailed-speed-counts/): An hourly aggregation of observed speeds for each location where a Watch Your Speed Program Sign was installed in 5 km/hr speed range increments.
  - [Safety Zone Watch Your Speed Program – Monthly Summary](https://open.toronto.ca/dataset/safety-zone-watch-your-speed-program-monthly-summary/): A summary of observed speeds for each location where a Safety Zone Watch Your Speed Program Sign was installed.
  - [Mobile Watch Your Speed Program – Detailed Speed Counts](https://open.toronto.ca/dataset/mobile-watch-your-speed-program-detailed-speed-counts/): An hourly aggregation of observed speeds for each sign installation in 5 km/hr speed range increments for each location where a Mobile Watch Your Speed Program Sign was installed.
  - [Mobile Watch Your Speed Program – Speed Summary](https://open.toronto.ca/dataset/mobile-watch-your-speed-program-speed-summary/): A summary of observed speeds for each location where a Mobile Watch Your Speed Program Sign was installed.

  ### Data Elements

The data is inserted into `wys.raw_data`. Data from the API is already pre-aggregated into roughly 5 minute bins.

|Field name|Data type|Description|Example|
|------|------|-------|------|
`raw_data_uid`|integer|A unique identifier for the `raw_data` table|2655075
`api_id`|integer|ID used for the API, and unique identifier for the `locations` table|1967
`datetime_bin`|timestamp|Start time of the bin|2018-10-29 10:00:00
`speed`|integer|Exact speed of the number of vehicles in `count`|47
`count`|integer|Number of vehicles in datetime_bin/api_id/speed combination|2
`counts_15min`|integer|A unique identifier for `counts_15min` table. Indicates if the data has already been processed or not.|150102

`wys.counts_15min` has aggregated 15 minute time bins, and aggregated 5 km/h speed bins by using the `aggregate_speed_counts_15min()` function. Values for the speed bins are replaced by lookup table IDS.

|Field name|Data type|Description|Example|
|------|------|-------|------|
`counts_15min`|integer|A unique identifier for the `counts_15min` table|2655075
`api_id`|integer|ID used for the API, and unique identifier for the `locations` table|1967
`datetime_bin`|timestamp|Start time of the 15 minute aggregated bin|2018-10-29 10:00:00
`speed_id`|integer|A unique identifier for the 5 minute speed bin in the `speed_bins` table|5
`count`|integer|Number of vehicles in datetime_bin/api_id/speed bin combination|7
