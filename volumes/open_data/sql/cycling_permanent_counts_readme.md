
# Permanent Bicycle Counters Open Data Dictionary 
<!-- Note: this data dictionary is converted to pdf via pandoc and published to Open Data via Airflow. -->

## cycling_permanent_counts_locations.csv

This table contains the locations and metadata about each permanent bicycle counting sensor installation. Table can be joined to daily and 15 minute tables using `location_dir_id`. This table references the City of Toronto's Street Centreline dataset. 

| Column Name              | Data Type   | Sample                                        | Description    | 
|-------------------------------|:-----------:|--------------------------------------:|-------------------------------------------------------------|
| location_dir_id            | integer        | 1 | Unique ID for location and direction for joining to data tables. |
| location_name            | text        | Bloor St E, West of Castle Frank Rd (retired) | Short description of sensor location. |
| direction                | text        | Eastbound                                     | Primary direction of travel associated with the bicycle count data. When bidirectional=True, this refers to the direction in which cyclists are actually traveling. When bidirectional=False, this refers to the primary direction of travel associated with the lane in which the detector is situated, typically determined by the side of the road or path on which the detector is located. |
| linear_name_full         | text        | Bloor St E                                    | Name of street on which the detector is located, or the nearest parallel street adjacent to the cycle track / multi-use path on which the detector is located. |
| side_street              | text        | Castle Frank Rd                               | Nearest cross street to the detector location. |
| bidirectional            | boolean     | False                                       |  Indicates whether opposing directions share the same path (True) or are separated by distance or a physical barrier (False) |
| longitude                | float     | -79.3681194                                   | Approximate longitude of sensor. |
| latitude                 | float     | 43.6738047                                    | Approximate latitude of sensor. |
| centreline_id            | integer     | 8540609                                       | Unique identifier of the [Toronto Centreline (TCL)](https://open.toronto.ca/dataset/toronto-centreline-tcl/) corresponding to the name of street on which the detector is located, or the nearest parallel street adjacent to the cycle track / multi-use path on which the detector is located. |
| bin_size                 | text    | 00:15:00 | Length of time bins into which count data are aggregated at the location in the "15-minute counts" tables. |
| latest_calibration_study | date        |                                               | Date of latest calibration study. Data at this location have been adjusted retroactively to this date to adjust for under- or overcount. If no date is shown, the sensor at the location has not been calibrated (data have not been adjusted). |
| first_active             | date        | 1994-06-26                                    | The earliest date for which data are available at the location. |
| last_active              | date        | 2019-06-13                                    | The most recent date for which data are available at the location. |
| date_decommissioned      | date        | 2019-06-13                                    | The date the sensor was decommissioned, if no longer active (retired). |
| technology               | text        | Induction - Other                             | Technology used to detect bicycles and other micromobility vehicles. |

<!-- \pagebreak used for pandoc formatting -->
\pagebreak

## cycling_permanent_counts_daily_counts.csv

Daily cycling and micromobility volumes by location and direction. 

| Column Name      | Data Type | Sample                                        | Description                              |
|------------------|:---------:|----------------------------------------------:|------------------------------------------|
| location_dir_id            | integer        | 1 | Unique ID for location and direction for joining to `cycling_permanent_counts_locations`. |
| location_name    | text                        | Bloor St E, West of Castle Frank Rd (retired) | Short description of sensor location.                                                        |
| direction        | text                        | Westbound                                     | Primary direction of travel associated with the bicycle count data. When bidirectional=True, this refers to the direction in which cyclists are actually traveling. When bidirectional=False, this refers to the primary direction of travel associated with the lane in which the detector is situated, typically determined by the side of the road or path on which the detector is located.                                                    |
| linear_name_full         | text        | Bloor St E                                    | Name of street on which the detector is located, or the nearest parallel street adjacent to the cycle track / multi-use path on which the detector is located. |
| side_street              | text        | Castle Frank Rd                               | Nearest cross street to the detector location. |
| dt               | date      | 06/26/1994                                    | Date of count.                           |
| daily_volume     | integer   | 939                                           | Total count of users in 24-hours on date `dt`, by direction.             |

## cycling_permanent_counts_15min_counts_YYYY_YYYY.csv

15 minute cycling and micromobility volumes by location and direction. Where 15 minute volumes are not available, 1 hour volumes are provided. The row counts in these files may exceed the limits for Excel. 

| Column Name      | Data Type | Sample                         | Description                              |
|------------------|:---------:|-------------------------------:|------------------------------------------|
| location_dir_id            | integer        | 1 | Unique ID for location and direction for joining to `cycling_permanent_counts_locations`. |
| datetime_bin     | timestamp                   | 06/26/1994 0:00 | The date-time at which the record begins. See `bin_size` in `sites` table for size of bin.                               |
| bin_volume       | integer                     | 3                                             | Count of users in `datetime_bin`.                                                            |
