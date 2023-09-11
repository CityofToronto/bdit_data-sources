# Short-term cycling volume counts

Short-term cycling count data collected by (what is now) the Cycling & Pedestrian Projects Unit is stored in the `cycling` schema. This data has not been updated since at least 2021.

There are two tables of note:

## `count_info`
| Field | Type | Description |
| --- | --- | --- |
| `count_id` | `integer` | Corresponds to a single day of counts at a single location |
| `centreline_id` | `integer` | ID of centreline segment the count occured on |
| `dir_id` | `integer` | -1/+1 as per Aakash and Sunny's definition |
| `dir` | `character varying` | EB/WB/NB/SB |
| `count_date` |  `date` | Date the count took place on |
| `temperature` | `integer` | Temperature on the day of the count in degrees C |
| `precipitation` | `integer` | Precipitation on the day of the count in mm |
| `count_type` | `character varying` | Automatic or manual |
| `filename` | `character varying` | Name of excel spreadsheet in open data |

## `count_data`
| Field | Type | Description |
| --- | --- | --- |
| `count_id` | `integer` | Used to join on `count_info` |
| `start_time` | `time` | Beginning of count window |
| `end_time` | `time` | Beginning of count window |
| `volume` | `integer` | Nuber of cyclists in count window |
