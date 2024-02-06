This folder contains sql scripts used in both the API and the old data dump
process. the [`csv_data/`](csv_data/) sub-folder contains `sql` files unique to
processing the data from csv dumps.

## 2. Table Structure

### Miovision Data Relationships at a Glance

![Miovision Data Entity Relationship Diagram](img/Mio_ERD.png)

### Key Tables 

- [`intersections`](#intersections)
- [`classifications`](#classifications)
- [`movements`](#movements)
- [Aggregated Data](#aggregated-data)
  - [`volumes_15min_mvt`](#volumes_15min_mvt): TMC-style 15-minute aggregated data
  - [`volumes_15min`](#volumes_15min): ATR-style 15-minute aggregated data
  - [`unacceptable_gaps`](#unacceptable_gaps)

#### `intersections`

Reference table for each unique intersection at which data has been collected, you can also see them [on this map.](geojson/miovision_intersections.geojson):

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
intersection_uid|integer|Unique identifier for table|10|
id|text|Unique id from Miovision API|990cd89a-430a-409a-b0e7-d37338394148|
intersection_name|text|Intersection in format of [main street] / [cross street]|King / Bathurst|
date_installed|date|Installation date of the camera (date of the first available timestamp)|2017-10-03|
date_decommissioned|date|Decommissioned date of the camera (date of the last available timestamp)|NULL|
lat|numeric|Latitude of intersection location|43.643945|
lng|numeric|Longitude of intersection location|-79.402667|
street_main|text|Name of primary street|King|
street_cross|text|Name of secondary street|Bathurst|
int_id|bigint|int_id linked to centrelines|13467722|
px|integer|px linked to traffic lights|201|
geom|geometry|Point geometry of that intersection|0101000020E61000006B0BCF4BC5D953C01CB62DCA6CD24540|
n_leg_restricted|boolean|Whether that leg is restricted to vehicles|NULL|
e_leg_restricted|boolean|Whether that leg is restricted to vehicles|NULL|
s_leg_restricted|boolean|Whether that leg is restricted to vehicles|NULL|
w_leg_restricted|boolean|Whether that leg is restricted to vehicles|NULL|


#### `classifications`

Reference table for all classifications:

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
classification_uid|serial|Unique identifier for table|2|
classification|text|Textual description of mode|Bicycles|
location_only|boolean|If TRUE, represents movement on crosswalk (as opposed to road)|FALSE|
class_type|text|General class category (Vehicles, Pedestrians, or Cyclists)|Cyclists|

Here is a description of the classification_uids and corresponding types. 
Note that bicycles are available at both a turning movement level and at an approach level. Approach level bicycle counts should be used for the large majority of applications as the data is considered more accurate.

 classification_uid | classification | definition / notes |
:-----|:-----|:-----|
1|Light|Cars and other passenger vehicles (like vans, SUVs or pick-up trucks)|
2|Bicycle|do not use - poor data quality. Tracks bicycle turning movements|
3|Bus|A large vehicle that provides transportation for many humans. Since 2019-08-22 this includes streetcars, however initially they were classified under `9`, MotorizedVehicle.|
4|SingleUnitTruck|A truck that has a non-detachable cab and trailer system|
5|ArticulatedTruck|A truck that has a detachable cab and trailer system|
6|Pedestrian|A walker. May or may not include zombies...|
7|Bicycle|Bicycle in crosswalk. Same `movement_uid`s as 6, Pedestrian. Unclear if it is necessarily being walked or ridden. **do not use** aggregate volumes will be removed from tables |
8|WorkVan|A van used for commercial purposes|
9|MotorizedVehicle|Miscellaneous vehicles. Prior to 2019-08-22 this included streetcars.|
10|Bicycle|Tracks bicycle entrances and exits. There are currently no exits in the aggregated tables. This classification is only available from 2021-07-11 on. Bicycle data is not great - stay tuned.|

#### `movements`

Reference table for road user movements:

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
movement_uid|integer|Identifier representing current turning movement|1|
movement_name|text|Short description of movement|thru|
crosswalk_movement|boolean|Whether the movement describes pedestrians on crosswalks|false|
movement_pretty_name|text|Long description of movement|Through|


Here is a description of the movement_uids and corresponding types:

**movement_uid**|**movement_pretty_name**|**definition**|
:-----|:-----|:-----|
1|Through|Vehicle drove through the intersection (no turns)|
2|Left|Vehicle turned left|
3|Right|Vehicle turned right|
4|U-Turn|Vehicle went back from whence it came - usually excluded from counts|
5|Clockwise|Pedestrian (or bike in crosswalk) proceeded clockwise around the intersection (a pedestrian on the north leg going clockwise would be heading eastbound)|
6|Counter Clockwise|Pedestrian (or bike in crosswalk) proceeded counter clockwise around the intersection (a pedestrian on the north leg going counter clockwise would be heading westbound)|
7|Bicycle Entrance|Used to determine where bicycles entered the intersection|
8|Bicycle Exit|Used to determine where bicycles exited the intersection|


#### Aggregated Data

Data are aggregated from 1-minute volume data into two types of 15-minute volume products: Turning Movement Count (TMC) [(in `volumes_15min_mvt`)](#volumes_15min_mvt) and Automatic Traffic Recorder (ATR) [(in `volumes_15min`)](#volumes_15min) equivalents. Have a look at [Understanding Legs, Movement and Direction of Travel in `getting_started.md`](getting_started.md#understanding-legs-movement-and-direction-of-travel) for a visual explanation of the differences between the two tables.

##### `volumes_15min_mvt`

`volumes_15min_mvt` contains data aggregated into 15 minute bins. In order to
make averaging hourly volumes simpler, the volume can be `NULL` (for all modes)
or `0` for classifications 1, 2, 6, 10 (which corresponds to light vehicles,
bicycles (classifications 2 and 10) and pedestrians).

The 1-min data do not identify if a camera is malfunctioning, so gaps in data
could either mean there was no volume, or that the camera malfunctioned. Because
we have continuous data from these counters, we no longer try to interpolate
data during gaps. When our heuristics identify `unacceptable_gaps`, then the
entire hour of data is thrown out and the volume is set to `NULL` to imply that
the data has been processed for this hour, but the results have been discarded.

A `0` value implies the process identifies the camera was working, but there was no volume for that mode. Only volumes for pedestrians, cyclists and light vehicles (`classification_uid IN (1,2,6,10)`) are filled in because those are the modes we report on more frequently. Other modes are not filled because they have much lower volumes, so the 0s would expand the size of the dataset considerably.

The [`aggregate_15_min_mvt()`](sql/function/function-aggregate-volumes_15min_mvt.sql) function performs zero-filling by cross-joining a table containing all possible movements described in ([`intersection_movements`](#intersection_movements)). The only type of movement tracked in the 1-minute volume data, but not the aggregated data, is bicycle exits (`classification_uid = 10 and movement_uid = 8`). The vendor recommended that bicycle exits not be used due to data quality concerns.

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
volume_15min_mvt_uid|integer|Unique identifier for table from sequence `miovision_api.volumes_15min_mvt_volume_15min_mvt_uid_seq`. |14524|
intersection_uid|integer|Identifier linking to specific intersection stored in `intersections`|31|
datetime_bin|timestamp without time zone|Start of 15-minute time bin in EDT|2017-12-11 14:15:00|
classification_uid|text|Identifier linking to specific mode class stored in `classifications`|1|
leg|text|Entry leg of movement|E|
movement_uid|integer|Identifier linking to specific turning movement stored in `movements`|2|
volume|integer|Total 15-minute volume|78|
processed|boolean| Flag if data has been aggregated to `miovision_15min`| TRUE

**Please note that movements for vehicles (including bicycles) are different than those for pedestrians.**

Please see [this diagram](getting_started.md#Vehicle-Movements) for a visualization of turning movements for vehicles (including bicycles) and [this diagram](getting_started.md#Pedestrian-Movement) for a visualization of pedestrian movements.

- A *Unique constraint* was added to `miovision_api.volumes_15min_mvt` table based on `intersection_uid`, `datetime_bin`, `classification_uid`, `leg` and `movement_uid`.

**NOTE:** data processing for each day happens from 23:00 the previous day to 22:59 current day. \
(23:00 datetime_bin contains 1-min bin >= 23:00 and < 23:15 whereas \
22:45 datetime_bin contains 1-min bin >= 22:45 and < 23:00)

##### `volumes_15min`

Data table storing ATR versions of the 15-minute turning movement data. Data in
`volumes` is stored in TMC format, so must be converted to ATR to be included in
`volumes_15min`.
                                                           
**ATR movements define leg as the approach direction of vehicles (like TMCs)**,
and **direction as the cardinal direction of traffic travelling through that
side of the intersection**. For a typical '+' intersection, there will be 8
possible ATR since there are 4 legs and 2 directions of travel for each ATR leg.

If you are having trouble picturing it, check out [this
diagram](getting_started.md#From-Movement-Counts-to-Segment-Counts).

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
volume_15min_uid|integer|Unique identifier for table from sequence `miovision_api.volumes_15min_volume_15min_uid_seq`. |12412|
intersection_uid|integer|Identifier linking to specific intersection stored in `intersections`|31|
datetime_bin|timestamp without time zone|Start of 15-minute time bin in EDT|2017-12-11 14:15:00|
classification_uid|text|Identifier linking to specific mode class stored in `classifications`|1|
leg|text|Segment leg of intersection|E|
dir|text|Direction of traffic on specific leg|EB|
volume|integer|Total 15-minute volume|107|

**NOTE:** data processing for each day happens from 23:00 the previous day to 22:59 current day. \
(23:00 datetime_bin contains 1-min bin >= 23:00 and < 23:15 whereas \
22:45 datetime_bin contains 1-min bin >= 22:45 and < 23:00)


[`miovision_api.movement_map`](#movement_map) is used to convert the TMC data to the ATR data. 

A *Unique constraint* was added to the `miovision_api.volumes_15min` table based on `intersection_uid`, `datetime_bin`, `classification_uid`, `leg` and `dir`.

##### `miovision_api.volumes_daily`

Daily volumes by intersection_uid, classification_uid. Excludes `anomalous_ranges` (use discouraged based on investigations) but does not exclude time around `unacceptable_gaps` (zero volume periods). 

| Field Name               | Comments                                                                   | Data Type   | Exmple     |
|:-------------------------|:---------------------------------------------------------------------------|:------------|:-----------|
| dt                       |                                                                            | date        | 2023-09-05 |
| intersection_uid         |                                                                            | integer     | 1          |
| classification_uid       |                                                                            | integer     | 1          |
| daily_volume             |                                                                            | integer     | 13830      |
| isodow                   | Use `WHERE isodow <= 5 AND holiday is False` for non-holiday weekdays.     | smallint    | 2          |
| holiday                  |                                                                            | boolean     | False      |
| datetime_bins_missing    | Minutes with zero vehicle volumes out of a total of possible 1440 minutes. | smallint    | 69         |
| unacceptable_gap_minutes | Periods of consecutive zero volumes deemed unacceptable based on avg intersection volume in that hour.                   | smallint    | 0          |
| avg_historical_gap_vol   | Avg historical volume for that classification and gap duration based on averages from a 60 day lookback in that hour.             | integer     |            |


##### `unacceptable_gaps`

Data table storing all the hours containing gaps larger than 5-20 minutes (this minimum threshold is set based on `gapsize_lookup`) for each intersection, including those with no data. Generated daily using the `find_gaps(start_date)` function. 
More information can be found at [#3. Finding gaps and malfunctioning camera](#3-finding-gaps-and-malfunctioning-camera). This table is used in the `aggregate_15_min_mvt` function to set any data during unacceptable hours to `null` to prevent it from being included in AVG calculations.

| Field Name                   | Comments                                                                      | Data Type                   | Example              |
|:------------------------------|:------------------------------------------------------------------------------|:----------------------------|:--------------------|
| dt                            | The date for which the function `miovision_api.find_gaps` was run to insert this row.            | date                        | 2023-09-05          |
| intersection_uid              |                                                                               | integer                     | 4                   |
| gap_start                     | The timestamp when the unacceptable gap starts.                               | timestamp without time zone | 2023-09-05 07:57:00 |
| gap_end                       | The timestamp when the unacceptable gap ends.                                 | timestamp without time zone | 2023-09-05 10:18:00 |
| gap_minutes_total             | Duration of gap in minutes (gap_end - gap_start)                              | integer                     | 141                 |
| allowable_total_gap_threshold | The minimum duration of zero volume to be considered an unacceptable gap for this intersection, hour, and day type (weekend/weekday) based on a 60 day lookback and calculated in miovision_api.gapsize_lookup_insert.     | integer                     | 5                   |
| datetime_bin                  | A 15 datetime_bin which falls within the gap, to be used for joining to volumes_15min* tables.                                                            | timestamp without time zone | 2023-09-05 07:45:00 |
| gap_minutes_15min             | The portion of the total gap which falls within the 15 minute bin starting with datetime_bin.            | integer                     | 3                   |

##### `gapsize_lookup`

Data table storing a 60 day lookback average hourly volume for each `intersection_uid`, `classification_uid`.
Used to determine the maximum acceptable gap for use in `unacceptable_gaps` table and to determine the average historical volume associated with those gaps by mode. 

| column_name        | Comments                                                                            | data_type   | sample             |
|:-------------------|:------------------------------------------------------------------------------------|:------------|:-------------------|
| dt                 | The date for which `miovision_api.gapsize_lookup_insert` was run to generate this row. The lookback period used to is the 60 days preceeding this date, matching the same day type (weekday/weekend)   | date        | 2023-11-01         |
| intersection_uid   | nan                                                                                 | integer     | 20                 |
| classification_uid | A null classification_uid refers to the total volume for that intersection which is used to determine the gap_tolerance.         | integer     | 5                  |
| hour_bin           | Hour of the day from 0-23.                                                          | smallint    | 18                 |
| weekend            | True if Saturday/Sunday or holiday (based on ref.holiday table).                    | boolean     | False              |
| avg_hour_vol       | The average volume for this hour/intersection/weekend combination based on a 60 day lookback.                  | numeric     | 1.6666666666666667 |
| gap_tolerance      | The minimum gap duration to be considered an unacceptable_gap. Only valid for the overall intersection volume (classification_uid IS NULL).           | smallint    |                    |

### `volumes`

Data table storing all 1-minute observations in its **transformed** form. Records represent total 1-minute volumes for each [intersection]-[classification]-[leg]-[turning movement] combination.  
Partitioned by year and month using declarative partitioning.  

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
volume_uid|serial|Unique identifier for table|5100431|
intersection_uid|integer|Identifier linking to specific intersection stored in `intersections`|31|
datetime_bin|timestamp without time zone|Start of 1-minute time bin in EDT|2017-10-13 09:07:00|
classification_uid|text|Identifier linking to specific mode class stored in `classifications`|1|
leg|text|Entry leg of movement|E|
movement_uid|integer|Identifier linking to specific turning movement stored in `movements`|2|
volume|integer|Total 1-minute volume|12|
volume_15min_mvt_uid|serial|Unenforced foreign key to [`volumes_15min_mvt`](#volumes_15min_mvt) The foreign key constraint was removed to support partitioning. |14524|

- A *Unique constraint* exists on `miovision_api.volumes` based on `intersection_uid`, `datetime_bin`, `classification_uid`, `leg`, and `movement_uid`.

### Reference Tables

#### `movement_map`

Reference table for transforming aggregated turning movement counts (see `volumes_15min_mvt`) into segment-level volumes (see `volumes_15min`):

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
leg_new|text|Intersection leg on which 15-minute volume will be assigned|E|
dir|text|Direction on which 15-minute volume will be assigned|EB|
leg_old|text|Intersection leg on which 15-minute turning movement volume is currently assigned|W|
movement_uid|integer|Identifier representing current turning movement - see `movements`|1|

Here are some example rows from the table:

|leg_new|dir|leg_old|movement_uid|description of movement|
|-------|---|-------|------------|-----------------------|
| E | EB | E | 4 | Approached intersection from the east leg, u-turned, exited intersection from the east leg
| E | EB | S | 3 | Approached intersection from the south leg, turned right, exited intersection from the east leg
| E | EB | W | 1 | Approached intersection from the west leg, proceeded straight ahead, exited intersection from the east leg
| E | EB | N | 2 | Approached intersection from the north leg, turned left, exited intersection from the east leg

- `leg_new` (leg for ATR) - anything that crosses that side of the intersection
- `dir` - heading of traffic crossing `leg_new`
- `leg_old` (leg for TMC) - direction the vehicles approach into intersection
- `movement_uid` - turning movement stored in `movements`

The example above represents a mapping from TMC to ATR `E` leg and `EB` direction. The blue and green arrows in [this diagram](getting_started.md#All-the-East-Leg-Crossings!) will help you visualize the movements described in the table.

#### `periods`

Reference table for all unique time periods. Used primarily to aggregate 15-minute data for reporting purposes:

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
period_id|integer|Unique identifier for table|3|
day_type|text|Day type for date filter|[Weekday OR Weekend]|
period_name|text|Textual description of period|14 Hour|
period_range|timerange|Specific start and end times of period|[06:00:00,20:00:00)|
report_flag|boolean|Indicates whether the period is used in a report|true|


#### `intersection_movements`

This was created using [`create-table-intersection_movements.sql`](sql/table/create-table-intersection_movements.sql) and is a reference table of all observed movements for each classification at each intersection. This is used in aggregating to the 15-minute TMC's in order to [fill in 0s in the volumes](#volumes_15min_mvt). Subsequently, movements present in the volumes data [which were erroneous](https://github.com/CityofToronto/bdit_data-sources/issues/144#issuecomment-419545891) were deleted from the table. This table will include movements which are illegal, such as left turns at intersections with turn restrictions but not movements like a turn onto the wrong way of a one-way street. It will need to be manually updated when a new location is added.

Since this reference table must be updated every time a new intersection is added, there are several iterations of it. The earliest is `miovision_api.intersection_movements_20200805`; the latest is `intersection_movements_20210712`. Users should use `intersection_movements` and may find their permissions restricted on the dated versions of this table.

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
 intersection_uid| integer | ID for intersection | 1 |
 classification_uid| integer | Identifier linking to specific mode class stored in `classifications`|1|
 leg| text | Entry leg of movement|E|
 movement_uid| integer | Identifier linking to specific turning movement stored in `movements`|2|

#### `centreline_miovision`

This table maps all miovision intersection legs to centreline street segments. It needs to be updated manually using [this script](sql/table/create-mv-mio_cent.sql) when intersections are added.

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
centreline_id| numeric | Corresponds to `geo_id` in `gis.centreline`|14016757|
intersection_uid| integer | ID for intersection | 1 |
leg| text | A segment that forms part of a miovision intersection, identified by its location relative to the centre of the intersection|W|

### Primary and Foreign Keys

To create explicit relationships between tables, `volumes`, `volume_15min_mvt`, `atr_mvt_uid` and `volume_15min` have primary and foreign keys. Primary keys are unique identifiers for each entry in the table, while foreign keys refer to a primary key in another table and show how an entry is related to that entry.

#### List of primary and foreign keys

* `volumes` has the primary key `volume_uid` and foreign key `volume_15min_mvt_uid` which refers to `volume_15min_mvt`
* `volumes_15min_mvt` has the primary key `volume_15min_mvt_uid`
* `volume_15min` has the primary key `volume_15min_uid` 

The current primary purpose for the keys is so that on deletion, the delete cascades through all tables. The keys also indicate whether it is new data if the foreign key is null, and tells the function to aggregate the data if it is new data. The keys can also be used in selecting data.

### Other Important Tables

The tables below are produced using functions explained in the [API Puller](api/readme.md#postgresql-functions). They produce a lookup table of date-intersection combinations to be used for checking purposes or even for formal reporting.

|Table|Purpose|
|------|-------|
|`api_log`|Contains a record of the `start_date` and `end_date` for an `intersection_uid` and when the data was pulled as `date_added`|
|`missing_dates`|Contains a record of the `intersection_uid` and the `dt` that were missing in the `volumes_15min` table, with `period_type` stated|
|`report_dates`|Contains a record for each intersection-date combination in which at least forty 15-minute time bins exist between 6AM and 8PM|


#### `volumes_mvt_atr_xover`

**This is a crossover table to link `volumes_15min_mvt` to the `volumes_15min` table**. As described above, the TMC to ATR relationship is a many to many relationship. The [`aggregate_15_min()`](sql/function/function-aggregate-volumes_15min.sql) function that populates `volumes_15min` also populates this table so that a record of which `volume_15min_mvt` bin corresponds to which `volume_15min` bin is kept, and vice versa. As a result, multiple entries of both `volume_15min_uid` and `volume_15min_mvt_uid` can be found in the query.

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
volume_15min_mvt_uid|int|Unique identifier for `volumes_15min_mvt` table|14524|
volume_15min_uid|serial|Unique identifier for table|12412|
## 3. Finding Gaps and Malfunctioning Camera

In order to better determine if a camera is still working, we have decided to use the gaps and islands method to figure where the gaps are (gaps as in the unfilled space or interval between the 1min bins; a break in continuity) and their sizes. There are two parts of this in the whole process.

### Part I - Unacceptable Gaps
The following process is used to determine camera wide data outages and then find out if the gaps are within the acceptable range or not. The timebins exceeding the allowed gap_size will then be inserted an `unacceptable_gaps` table. Finding gaps is important so that we know how reliable the data is for that time period based on past volume and not include those bins found within the unacceptable gaps range.

1. Within `find_gaps` function, the `gapsize_lookup_insert` function is run to populate the table `gapsize_lookup` with acceptable gap sizes based on avg hourly volumes from a 60 day rolling lookback. 
  - The set of acceptable `gap_tolerance` implemented is based on an investigation stated in this [notebook](dev_notebooks/volume_vs_gaps.ipynb). 
2. Then, the function [`miovision_api.find_gaps`](sql/function-find_gaps.sql) identifies all gaps of data in the table `miovision_api.volumes` and check if they are within the acceptable range of gap sizes or not based on the information from the `gapsize_lookup` table above. Sensors with no data are not identified - these will instead be included in `anomalous_ranges` table with PR [#777](https://github.com/CityofToronto/bdit_data-sources/pull/777). 
3. Unacceptable gaps are cross joined to 15 minute bins and inserted into the table [`miovision_api.unacceptable_gaps`](#unacceptable_gaps). 
4. Based on the `unacceptable_gaps` table, [`aggregate_15min_mvt`](#volumes_15min_mvt) function will not aggregate 1min bins found within within 15 minute periods matching unacceptable_gaps' `datetime_bin`.

### Part II - Working Machine
The following process is to determine if a Miovision camera is still working. It is different from the process above because the gap sizes used above are small and do not say much about whether a camera is still 
working. We roughly define a camera to be malfunctioning if that camera/intersection has a gap greater than 4 hours. This is checked daily by `miovision_check` DAG, task `check_gaps`. A slack notification will be sent if there's at least 1 camera that is not working. The function also returns a list of intersections that are not working and from what time to what time that the gaps happen which is helpful in figuring out what has happened.

### Identifying Questionable Data Quality

The table `miovision_api.anomalous_ranges` is used to flag times and places in the data where counts are potentially questionable, suspicious, or irregular. Sometimes counts are clearly not valid or data are missing, and sometimes counts look very weird but have been investigated and confirmed as valid or subject to some specific caveat. This table helps us track the status of these investigations and their results. 
The basic idea is to identify sections of data (by timerange, intersection, and classification) that have been flagged as suspicious/anomalous for whatever reason, and state what we know about that data-subset in a semi-structured way. This lets us further investigate where that is required and carve out data that has been formally cast into doubt from queries that need only-the-best.

#### Fields in `miovision_api.anomalous_ranges`
| Column | Description |
| ------ | ----------- |
| uid    | simple incrementing primary key |
| intersection_uid | the intersection; `NULL` if applies to all intersections, e.g. an algorithm change |
| classification_uid | the classification; `NULL` if applies to all classifications e.g. a badly misaligned camera |
| range_start | the beginning of the anomalous range in question; may be open-ended (NULL). Inclusive. The precision here is to the second, so if you're unsure about alignment with time bins, it may be best to be conservative with this and extend the range_start slightly _before_ the problem area. |
| range_end | the end of the anomalous range in question; may be open-ended (NULL). Exclusive. The precision here is to the second, so if you're unsure about alignment with time bins, it may be best to be conservative with this and extend the range_end slightly _past_ the problem area. |
| notes | as detailed a description of the issue as reasonably possible; if there are unknowns or investigations in progress, describe them here also |
| investigation_level | references `miovision_api.anomaly_investigation_levels`; indicates the degree to which the issue has been investigated. Is it just a suspicion? Has it been authoritatively confirmed? Etc. |
| problem_level | references `miovision_api.anomaly_problem_levels`; indicates the degree or nature of the problem. e.g. valid with a caveat vs do-not-use under any circumstance |

#### Fields in `miovision_api.anomaly_investigation_levels` and `miovision_api.anomaly_problem_levels`
| Column | Description |
| ------ | ----------- |
| uid    | very short descriptive text; primary key |
| description | full description / documentation of the category; refer directly to these tables for documentation of the available classifications. |

#### An applied example

When looking for only "typical" data, `anomalous_ranges` should be used along with tables like `ref.holiday` to filter data. 

```sql
SELECT volume_uid
FROM miovision_api.volumes
WHERE 
    NOT EXISTS ( -- this is our one big filter for bad/dubious data
        SELECT 1
        FROM miovision_api.anomalous_ranges
        WHERE
            anomalous_ranges.problem_level IN ('do-not-use', 'questionable')
            AND (
                volumes.datetime_bin >= anomalous_ranges.range_start
                OR anomalous_ranges.range_start IS NULL
            ) AND (
                volumes.datetime_bin < anomalous_ranges.range_end
                OR anomalous_ranges.range_end IS NULL
            ) AND (
                anomalous_ranges.intersection_uid = volumes.intersection_uid
                OR anomalous_ranges.intersection_uid IS NULL
            ) AND (
                anomalous_ranges.classification_uid = volumes.classification_uid
                OR anomalous_ranges.classification_uid IS NULL
            )
    )
    AND NOT EXISTS ( -- also exclude official holidays
        SELECT 1 FROM ref.holiday WHERE holiday.dt = volumes.datetime_bin::date
    )
    AND etc.
```

#### Identifying new anomalies
The `anomalous_ranges` table is populated in different ways:
  - manually by `miovision_data_detectives` after visual inspection or prompting from an Airflow alert
  - automatically by a [daily script](sql/function/function-identify-zero-counts.sql) which identifys zero volume days by intersection/classification.
    - There is an intention to eventually flag more unusual volumes automagically (see [Issue #630](https://github.com/CityofToronto/bdit_data-sources/issues/630)). 
  - initially some records were added after some manual QC work which can can be found in the [dev_notebooks README.md](dev_notebooks/README.md) (including notebooks and code).
  
## 4. Repulling data
### Deleting data to re-run the process

The Miovision ETL DAG `miovision_pull` and the command line `run_api` method, both have deletes built in to each insert/aggregation function. This makes both of these methods idempotent and safe to re-run without the need to manually delete data before re-pulling. Both methods also have an optional intersection_uid parameter which allows re-pulling or re-aggregation of a single intersection or a subset of intersections. 

Neither method supports deleting and re-processing data that is not in **daily blocks** (for example we cannot delete and re-pull data from `'2021-05-01 16:00:00'` to `'2021-05-02 23:59:00'`, instead we must do so from `'2021-05-01 00:00:00'` to `'2021-05-03 00:00:00'`).
