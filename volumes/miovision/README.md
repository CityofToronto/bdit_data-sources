# Miovision - Multi-modal Permanent Video Counters

## Table of Contents

1. [Overview](#1-overview)
2. [Table Structure](#2-table-structure)
3. [Technology](#3-technology)
4. [Processing Data from CSV Dumps](#4-processing-data-from-csv-dumps)
5. [Processing Data from API](#5-processing-data-from-api)
6. [Filtering and Interpolation](#6-filtering-and-interpolation)

## 1. Overview

(to be filled in)

## 2. Table Structure

### Original Data

#### `raw_data`
Data table storing all 1-minute observations in its **original** form. Records represent total 1-minute volumes for each [intersection]-[classification]-[leg]-[turning movement] combination. All subsequent tables are derived from the records in this table.

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
study_id|bigint|Unique identifier representing a specific intersection-date combination|474678|
study_name|text|Intersection in format of [main street] / [cross street]|King / Bathurst|
lat|numeric|Latitude of intersection location|43.643945|
lng|numeric|Longitude of intersection location|-79.402667|
datetime_bin|timestamp with time zone|Start of 1-minute time bin|2017-10-13 14:07:00+00|
classification|text|Specific mode class (see `classifications` below)|Lights|
entry_dir_name|text|Entry leg of movement|E|
entry_name|text|(not currently populated)||
exit_dir_name|text|Exit leg of movement|W|
exit_name|text|(not currently populated)||
movement|text|Specific turning movement (see `movements` below)|thru|
volume|integer|Total 1-minute volume|12|


### Reference Tables

#### `classifications`
Reference table for all 7 classifications: Lights, Bicycles on Road, Buses, Single-Unit Trucks, Articulated Trucks, Pedestrians on Crosswalk, and Bicycles on Crosswalk.

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
classification_uid|serial|Unique identifier for table|2|
classification|text|Textual description of mode|Bicycles|
location_only|boolean|If TRUE, represents movement on crosswalk (as opposed to road)|FALSE|
class_type|text|General class category (Vehicles, Pedestrians, or Cyclists)|Cyclists|


#### `intersections`
Reference table for each unique intersection at which data has been collected.

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
intersection_uid|serial|Unique identifier for table|10|
intersection_name|text|Intersection in format of [main street] / [cross street]|King / Bathurst|
street_main|text|Name of primary street|King|
street_cross|text|Name of secondary street|Bathurst|
lat|numeric|Latitude of intersection location|43.643945|
lng|numeric|Longitude of intersection location|-79.402667|


#### `movement_map`
Reference table for transforming aggregated turning movement counts (see `volumes_15min_tmc`) into segment-level volumes (see `volumes_15min`).

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
leg_new|text|Intersection leg on which 15-minute volume will be assigned|E|
dir|text|Direction on which 15-minute volume will be assigned|EB|
leg_old|text|Intersection leg on which 15-minute turning movement volume is currently assigned|W|
movement_uid|integer|Identifier representing current turning movement|1|


#### `movements`
Reference table for all unique movements: through, left turn, right turn, u-turn, clockwise movement on crosswalk, and counter-clockwise movement on crosswalk.

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
movement_uid|serial|Unique identifier for table|3|
movement|text|Textual description of specific turning movement|right|
location_only|boolean|If TRUE, represents movement on crosswalk (as opposed to road)|FALSE|


#### `periods`
Reference table for all unique time periods. Used primarily to aggregate 15-minute data for reporting purposes.

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
period_id|integer|Unique identifier for table|3|
day_type|text|Day type for date filter|[Weekday OR Weekend]|
period_name|text|Textual description of period|14 Hour|
period_range|timerange|Specific start and end times of period|[06:00:00,20:00:00)|


### Disaggregate Data

#### `volumes`
Data table storing all 1-minute observations in its **transformed** form. Records represent total 1-minute volumes for each [intersection]-[classification]-[leg]-[turning movement] combination.

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
volume_uid|serial|Unique identifier for table|5100431|
intersection_uid|integer|Identifier linking to specific intersection stored in `intersections`|31|
datetime_bin|timestamp without time zone|Start of 1-minute time bin in EDT|2017-10-13 09:07:00|
classification_uid|text|Identifier linking to specific mode class stored in `classifications`|1|
leg|text|Entry leg of movement|E|
movement_uid|integer|Identifier linking to specific turning movement stored in `movements`|2|
volume|integer|Total 1-minute volume|12|

### Aggregated Data

#### `volumes_15min_tmc`
Data table storing aggregated 15-minute turning movement data. 

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
volume_15min_tmc_uid|serial|Unique identifier for table|14524|
intersection_uid|integer|Identifier linking to specific intersection stored in `intersections`|31|
datetime_bin|timestamp without time zone|Start of 15-minute time bin in EDT|2017-12-11 14:15:00|
classification_uid|text|Identifier linking to specific mode class stored in `classifications`|1|
leg|text|Entry leg of movement|E|
movement_uid|integer|Identifier linking to specific turning movement stored in `movements`|2|
volume|integer|Total 15-minute volume|78|

#### `volumes_15min`
Data table storing aggregated 15-minute segment-level data.

**Field Name**|**Data Type**|**Description**|**Example**|
:-----|:-----|:-----|:-----|
volume_15min_uid|serial|Unique identifier for table|12412|
intersection_uid|integer|Identifier linking to specific intersection stored in `intersections`|31|
datetime_bin|timestamp without time zone|Start of 15-minute time bin in EDT|2017-12-11 14:15:00|
classification_uid|text|Identifier linking to specific mode class stored in `classifications`|1|
leg|text|Segment leg of intersection|E|
dir|text|Direction of traffic on specific leg|EB|
volume|integer|Total 15-minute volume|107|

### Important Views

## 3. Technology

(to be filled in)

## 4. Processing Data from CSV Dumps

### A. Populate `raw_data`
Ensure that `raw_data` has been updated with all new data (either from new collection periods, or replacing older data with corrected 1-minute observations).

1. Make a backup copy of `raw_data` using the following:

	`CREATE TABLE miovision.raw_data_old AS SELECT * FROM miovision.raw_data`

	You can delete this copy after fully QCing the dataset.
	
2. Delete any studies / data that is being replaced by new data (if data only covers a new set of dates, ignore this step).

3. Import new dataset using PostgreSQL COPY functionality into `raw_data`.

### b. Populate `volumes`
This will transform 1-minute data from `raw_data` into a standard normalized structure stored in `volumes`.

1. Run `populate-table-volumes.sql`. This will truncate `volumes` and replace with data from `raw_data`.
2. Ensure that the number of records in `volumes` is identical to that in `raw_data`. If a discrepancy exists, investigate further.

### c. Populate `volumes_15min_tmc` and `volumes_15min`
This will aggregate the 1-minute data from `volumes` into 15-minute turning movement counts (stored in `volumes_15min_tmc`) and segment-level counts (stored in `volumes_15min`). This process also filter potential partial 1-minute data and interpolates missing records where possible (see [Section 6](#6-filtering-and-interpolation))

1. Run `populate-table-volumes_15_min_tmc.sql`. This produces 15-minute aggregated turning movement counts with filtering and interpolation with gap-filling.
2. Run `populate-table-volumes_15_min.sql`. This produces 15-minute aggregated segment-level (i.e. ATR) data, while also producing 0-volume records for intersection-leg-dir combinations that don't have volumes (to allow for easy averaging).

### d. Populate `report_dates`
This produces a lookup table of date-intersection combinations to be used for formal reporting (this filters into various views).

1. Run `populate-table-report_dates.sql`. This creates a record for each intersection-date combination in which at least **forty** 15-minute time bins exist. There are exceptions which are explicitly removed at the end of the query.
2. If needed, modify query once the data has undergone QC if specific intersection-date combinations need to be removed, and re-run `populate-table-report_dates.sql`.

### e. Refresh reporting views
Refresh the relevant materialized views for monthly reporting:

- `miovision.report_volumes_15min`
- `miovision.volumes_15min_by_class`

### f. Produce summarized monthly reporting data

1. Run `miovision.report_summary` and copy over to relevant reporting templates.

## 5. Processing Data from API

(to be filled in)

## 6. Filtering and Interpolation

(to be filled in)