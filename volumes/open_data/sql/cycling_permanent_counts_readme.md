
# Tables

## ecocounter.cycling_permanent_counts_locations

| column_name              | data_type   | sample                                        | explanation    | 
|-------------------------------|:-----------:|--------------------------------------:|-------------------------------------------------------------|
| location_name            | text        | Bloor St E, West of Castle Frank Rd (retired) | Short description of sensor location. |
| direction                | text        | Eastbound                                     | Closest cardinal direction of bike flow. |
| linear_name_full         | text        | Bloor St E                                    | Linear name full from Toronto Centreline (TCL) |
| side_street              | text        | Castle Frank Rd                               | Nearest side street to sensor flow. |
| lng                      | numeric     | -79.3681194                                   | Approximate longitude of sensor. |
| lat                      | numeric     | 43.6738047                                    | Approximate latitude of sensor. |
| centreline_id            | integer     | 8540609                                       | centreline_id corresponding to Toronto Centreline (TCL) |
| bin_size                 | interval    | 0 days 00:15:00                               | Size of smallest datetime bin recorded by sensor. |
| latest_calibration_study | date        |                                               | Date of latest calibration study. |
| first_active             | date        | 1994-06-26                                    | The earliest date the sensor produced data. |
| last_active              | date        | 2019-06-13                                    | The most recent date of available data produced by the sensor. |
| date_decommissioned      | date        | 2019-06-13                                    | Date decommissioned. |
| technology               | text        | Induction - Other                             | Technology of permanent sensor. |

## open_data.cycling_permanent_counts_daily_counts

| column_name      | data_type | sample                                        | explanation                              |
|------------------|:---------:|----------------------------------------------:|------------------------------------------|
| location_name    | text      | Bloor St E, West of Castle Frank Rd (retired) | Short description of sensor location.    |
| direction        | text      | Westbound                                     | Closest cardinal direction of bike flow. |
| dt               | date      | 06/26/1994                                    | Date of count.                           |
| daily_volume     | integer   | 939                                           | Count of users on date `dt`.             |

## ecocounter.cycling_permanent_counts_15min_counts

| column_name      | data_type | sample                         | explanation                              |
|------------------|:---------:|-------------------------------:|------------------------------------------|
| location_name | text                        | Bloor St E, West of Castle Frank Rd (retired) | Short description of sensor location.                                                        |
| direction        | text                        | Westbound                                     | Closest cardinal direction of bike flow.                                                     |
| datetime_bin     | timestamp | 06/26/1994 0:00                               | The date-time at which the record begins. See `bin_size` in `sites` table   for size of bin. |
| bin_volume       | integer                     | 3                                             | Count of users in `datetime_bin`.                                                            |

