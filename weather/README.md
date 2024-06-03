# Weather Data
## Table of Content
- [Overview](#overview)
- [Historical Data](#historical-data)
    - [Table Structure](#table-structure)
- [Forecast Data](#forecast-data)
    - [Table Structure](#table-structure)
- [Data Pipeline](#data-pipeline)
- [Backfill Data](#backfill-data)
- [Manually Accessing Data](#manually-accessing-data)

## Overview
Weather has an undeniable effect on the transportation network, influencing people's behaviour, impacting capacity, and increasing the likelihood of collisions. We import two types of weather data from Environment Canada to our database, which includes historical data for both City of Toronto and Toronto Pearson Airport, and also forecast data for City of Toronto. These data are stored in the `weather` schema, maintained by `weather_admins`, and accessibile for all `bdit_humans` to view. 

## Historical Data

We import the historical data for two locations on a daily basis, City of Toronto (Station_id = 31688) and Toronto Pearson Airport (Station_id = 51459). Weather data for historical weather tables are pulled from the Government of Canada's "Historical Data" weather page, found [here](https://climate.weather.gc.ca/historical_data/search_historic_data_e.html)

**Please note** that the `total_rain` and `total_snow` fields for the city weather station are always `NULL`. All other fields can be accessed in the table `weather.historical_daily_city`. As an approximation, we import Toronto Pearson Airport's total rainfall and snowfall in a seperate table `weather.historical_daily_airport`. 

### Table Structure
| Column name  | Description                                                                                                                      | example    |
|--------------|----------------------------------------------------------------------------------------------------------------------------------|------------|
| dt           | Date                                                                                                                             | 2023-05-01 |
| temp_max     | The   highest temperature in degrees Celsius (°C) observed                                                                       | 10.7       |
| temp_min     | The   lowest temperature in degrees Celsius (°C) observed                                                                        | 6          |
| mean_temp    | The   mean temperature in degrees Celsius (°C) is defined as the average of the   maximum and minimum temperature                | 8.4        |
| total_rain   | The   total rainfall, or amount of all liquid precipitation in millimetres (mm)   such as rain, drizzle, freezing rain, and hail | 1.3        |
| total_snow   | The   total snowfall, or amount of frozen (solid) precipitation in centimetres   (cm), such as snow and ice pellets              | 0          |
| total_precip | The   sum of the total rainfall and the water equivalent of the total snowfall in   millimetres (mm)                             | 1.3        |


## Forecast Data

Forecast Data is inserted into `weather.prediction_daily` on a daily basis from Environment Canada's [Local Forecast website](https://weather.gc.ca/city/pages/on-143_metric_e.html). Location is set as Toronto, ON with `s0000458` station_id. Every day we pull forecast data for tomorrow and 4 days in the future (e.g. If today is Monday, we will pull in data for Tuesday to Saturday). Since past forecast data is not being stored in Environment Canada, this task cannot be backfilled. Thus, we pull 5 future days to limit the chance of not having any data for days when the pipeline fails.

### Table Structure
| Column name        | Description                                                       | example                                                            |
|--------------------|-------------------------------------------------------------------|--------------------------------------------------------------------|
| dt                 | Date                                                              | 05/04/2023                                                         |
| temp_max           | Forecasted highest temperature in degrees Celsius (°C) of the day | 16                                                                 |
| temp_min           | Forecasted lowest temperature in degrees Celsius (°C) of the day  | 6                                                                  |
| precip_prob_day    | Forecasted chance of precipitation before noon in percentage (%)  | 30                                                                 |
| precip_proc_night  | Forecasted chance of precipitation after noon in percentage (%)   | 0                                                                  |
| text_summary_day   | Detailed forecast summary before noon                             | A mix of sun and cloud with 30 percent chance of showers. High 16. |
| text_summary_night | Detailed forecast summary after noon                              | Cloudy periods. Low 6.                                             |
| date_pulled        | Day of when the data is pulled                                    | 05/03/2023                                                         |


## Data Pipeline

The data pipeline runs at 2:30 AM daily on airflow with the DAG `weather_pull`. There are four tasks: 1) `no_backfill` 2), `pull_prediction`, 3) `pull_historical_city`, and 4) `pull_historical_airport`. All tasks are run by `weather_bot`.
Note: Around 2024-06-03, the weather DAG was renamed from `pull_weather` with [minor changes](https://github.com/CityofToronto/bdit_data-sources/pull/976). 

![image](https://user-images.githubusercontent.com/46324452/235770699-275ea663-5035-4799-984b-5eb0e09878b1.png)

1) `no_backfill`

Uses `LatestOnlyOperator` that disable downstream tasks for backfill. This is set as an upstream for `pull_prediction` as forecast data cannot be backfilled.

2) `pull_prediction`

Runs script `prediction_import.py` which uses package `env_canada` to pull City of Toronto's forecast data of the next 5 days and insert into `weather.prediction_daily`. 

2) `pull_historical_city`

Runs script `historical_scrape.py` which uses package [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) to parse the HTML content returned from [request](https://docs.python-requests.org/en/master/user/quickstart/#make-a-request) for City of toronto. The day before execution date's data will be pulled and inserted into `weather.historical_daily_city`.


3) `pull_historical_airport`

Runs script `historical_scrape.py` which uses package [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/bs4/doc/) to parse the HTML content returned from [request](https://docs.python-requests.org/en/master/user/quickstart/#make-a-request) for Toronto Pearson Airport. The day before execution date's data will be pulled and inserted into `weather.historical_daily_airport`.

## Backfill Data

As mentioned before, only historical data can be backfilled. Other than backfilling from airflow's cli, you can also use the script `backfill_historical.py` for when you backfill a lot of dates (since you will have to backfill day by day in airflow). Note: Make sure you have a `db.cfg` in your home folder, and you have `weather_admins` permissions.

Input Params:
1) `start_dt`: The start date of the date range you want to backfill historical data, inclusive. 
2) `end_dt`: The end date of the date range you want to backfill historical data, exclusive.
3) `station_id`: The station_id to backfill, e.g. City of Toronto is station_id = 31688 and Pearson Airport is station_id = 51459

For example, if you want to backfill the entire month of 2022 March for Pearson Airport, you would run:
```
python3 backfill_historical -s 2022-03-01 -e 2022-04-01 -i 51459
```

## Manually Accessing Data
To access weather data outside of the stations & dates provided in the database, use the following links:

Access Environment Canada's [**Historical Data**](https://climate.weather.gc.ca/historical_data/search_historic_data_e.html) Website. Then, proceed with the following stpes:

1. In the search boxes, enter the desired parameters, including station and date range:
![historical1](https://github.com/CityofToronto/bdit_data-sources/assets/10802231/a899a6f7-beee-451d-b9a3-916be7cf76d0)


2. Select the desired station from the list (there can be quite a few, so try limiting the search 'year' in the previous step. Then, select 'Daily' from the data dropdown (hourly and monthly data can also be accessed) , and desired month/year:
![historical2](https://github.com/CityofToronto/bdit_data-sources/assets/10802231/e54cb6b4-cfb9-458e-90ab-4ae1018ed7a1)


3. View data (same format as in the weather schema):
![historical3](https://github.com/CityofToronto/bdit_data-sources/assets/10802231/1195b550-b8f8-451a-a1a9-96c04ae811b9)


### Uploading Manually Accessed Historical Data into the Database
If the backfill process is not working, or cannot find the desired data, historical data can also be imported into postgres, using the `csv_import.py` script. 

**Please Note** that this script is only to be used as a last resort. It does not contain any backstops for duplicate/NULL entries, broken data, etc. Raw data and duplicates will have to be checked manually.

1. Visit the desired weather station page, containing historical data, using the steps above. Then, you will see this prompt:
![historical4](https://github.com/CityofToronto/bdit_data-sources/assets/10802231/c1b08bfe-ae12-4487-9bb4-efed3602fb64)

    From here, you can select the format to download an entire year's worth of weather data for the current station. For our purposes, we will download in the CSV format.

2. Open the script `csv_import.py` in a text editor. Here, on Line 24, you can change the directoryto match the filepath of the CSV just downloaded. If uploading several weather CSV files, you can create a new folder containg those files - the script will process all CSV files.
![csvimport2](https://github.com/CityofToronto/bdit_data-sources/assets/10802231/23d24ee8-f7f2-4388-a8fc-225ee1c90ac7)

3. On Line 30, the table to insert into can be changed. 
![csvimport1](https://github.com/CityofToronto/bdit_data-sources/assets/10802231/2b959cb9-e39e-417f-b6b6-2a6dddea67d9)

4. Navigate to the script directory in a new terminal and run the script using `python csv_import.py`
