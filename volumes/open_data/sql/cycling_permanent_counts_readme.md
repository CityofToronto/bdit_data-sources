
# Permanent Bicycle Counters Open Data Dictionary 

## cycling_permanent_counts_locations.csv

This table contains the locations and metadata about each permanent bicycle counting sensor installation. Table can be joined to daily and 15 minute tables using `location_name` and `direction`. This table references the City of Toronto's Street Centreline dataset. 

| Column Name              | Data Type   | Sample                                        | Desciption    | 
|-------------------------------|:-----------:|--------------------------------------:|-------------------------------------------------------------|
| location_name            | text        | Bloor St E, West of Castle Frank Rd (retired) | Short description of sensor location. |
| direction                | text        | Eastbound                                     | Closest cardinal direction of bike flow. |
| linear_name_full         | text        | Bloor St E                                    | Linear name full from Toronto Centreline (TCL) |
| side_street              | text        | Castle Frank Rd                               | Nearest side street to sensor flow. |
| longitude                | float     | -79.3681194                                   | Approximate longitude of sensor. |
| latitude                 | float     | 43.6738047                                    | Approximate latitude of sensor. |
| centreline_id            | integer     | 8540609                                       | centreline_id corresponding to [Toronto Centreline (TCL)](https://open.toronto.ca/dataset/toronto-centreline-tcl/) |
| bin_size                 | text    | 00:15:00 | Duration of `datetime_bin`s recorded by sensor in the 15 minute table. |
| latest_calibration_study | date        |                                               | Date of latest calibration study. Where older sites have `null` values, the data was validated with other available sources. |
| first_active             | date        | 1994-06-26                                    | The earliest date for which data is available. |
| last_active              | date        | 2019-06-13                                    | The most recent date of available data produced by the sensor. |
| date_decommissioned      | date        | 2019-06-13                                    | Date decommissioned. |
| technology               | text        | Induction - Other                             | Technology of permanent sensor. |

## cycling_permanent_counts_daily_counts.csv

Daily cycling and micromobility volumes by location and direction. 

| Column Name      | Data Type | Sample                                        | Desciption                              |
|------------------|:---------:|----------------------------------------------:|------------------------------------------|
| location_name    | text      | Bloor St E, West of Castle Frank Rd (retired) | Short description of sensor location.    |
| direction        | text      | Westbound                                     | Closest cardinal direction of bike flow. |
| dt               | date      | 06/26/1994                                    | Date of count.                           |
| daily_volume     | integer   | 939                                           | Count of users on date `dt`.             |

<!-- \pagebreak used for pandoc formatting -->
\pagebreak

## cycling_permanent_counts_15min_counts_YYYY_YYYY.csv

15 minute cycling and micromobility by location and direction. Where 15 minute volumes are not available, 1 hour volumes are provided. 

| Column Name      | Data Type | Sample                         | Desciption                              |
|------------------|:---------:|-------------------------------:|------------------------------------------|
| location_name    | text                        | Bloor St E, West of Castle Frank Rd (retired) | Short description of sensor location.                                                        |
| direction        | text                        | Westbound                                     | Closest cardinal direction of bike flow.                                                     |
| datetime_bin     | timestamp                   | 06/26/1994 0:00 | The date-time at which the record begins. See `bin_size` in `sites` table for size of bin.                               |
| bin_volume       | integer                     | 3                                             | Count of users in `datetime_bin`.                                                            |
</nobr>
