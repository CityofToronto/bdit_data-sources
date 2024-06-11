<!-- TOC -->

- [About the Archive](#about-the-archive)
	- [7. Processing Data from CSV Dumps (NO LONGER IN USE)](#7-processing-data-from-csv-dumps-no-longer-in-use)
		- [`raw_data`](#raw_data)
		- [A. Populate `volumes`](#a-populate-volumes)
		- [B. Populate `volumes_15min_tmc` and `volumes_15min`](#b-populate-volumes_15min_tmc-and-volumes_15min)
		- [C. Refresh reporting views](#c-refresh-reporting-views)
		- [D. Produce summarized monthly reporting data](#d-produce-summarized-monthly-reporting-data)
		- [Deleting Data](#deleting-data)
	- [8. Filtering and Interpolation (NO LONGER IN USE)](#8-filtering-and-interpolation-no-longer-in-use)
		- [Filtering](#filtering)
		- [Interpolation](#interpolation)
	- [9. QC Checks](#9-qc-checks)
		- [Variance check](#variance-check)
		- [Invalid Movements](#invalid-movements)
	- [10. Open Data](#10-open-data)
	- [11. Deprecated Airflow DAGs](#11-deprecated-airflow-dags)
		- [**`pull_miovision`**](#pull_miovision)
		- [**`check_miovision`**](#check_miovision)

<!-- /TOC -->
# About the Archive

**This document contains information that has been deleted from other Miovision readmes**
It may prove useful to you at some time, perhaps in a somewhat bizarre set of circumstances.

## 7. Processing Data from CSV Dumps (NO LONGER IN USE)

Prior to the API pipeline being set up, we received data from Miovision in CSV
files for individual months of data collection, this is the procedure for
processing that data, relevant sql can be found in [`sql/csv_data`](sql/csv_data/).

### `raw_data`

Representation of 1-minute observations in their **original** form. Records represent total 1-minute volumes for each [intersection]-[classification]-[leg]-[turning movement] combination. New csv data shoudl be inserted into this table, but [a trigger](sql/trigger-populate-volumes.sql) will transform it into the [`volumes`](#volumes) format and leave this table empty.

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
movement|text|Specific turning movement (see `movement_map` below)|thru|
volume|integer|Total 1-minute volume|12|

### A. Populate `volumes`

With [`trigger-populate-volumes.sql`](sql/trigger-populate-volumes.sql), it is no longer necessary that a copy of the csv dump is on the database. To upload the csv dump, insert the file to `raw_data`. The trigger will transform 1-minute data from `raw_data` into a standard normalized structure stored in `volumes`, it returns null so no data actually gets inserted to `raw_data`.

1. The trigger function [`trigger-populate-volumes.sql`](sql/trigger-populate-volumes.sql) automatically populates `volumes` with new data from `raw_data`. `volumes` is the same data, except most of the information is replaced by integers that are referenced in lookup tables.
2. Ensure that the number of new records in `volumes` is identical to that in the csv dump (`WHERE volume_15min_tmc_uid IS NULL`). If a discrepancy exists, investigate further.

### B. Populate `volumes_15min_tmc` and `volumes_15min`

This will aggregate the 1-minute data from `volumes` into 15-minute turning movement counts (stored in [`volumes_15min_tmc`](#volumes_15min_tmc)) and segment-level counts (stored in [`volumes_15min`](#volumes_15min)). This process also filter potential partial 1-minute data and interpolates missing records where possible (see [Section 6](#6-filtering-and-interpolation))

1. Run [`SELECT mioviosion.aggregate_15_min_tmc();`](sql/function-aggregate-volumes_15min_tmc.sql). This produces 15-minute aggregated turning movement counts with filtering and interpolation with gap-filling for rows which have not yet been aggregated (the `FOREIGN KEY volume_15min_tmc_uid` is NULL).  Additionally, this query produces 0-volume records for intersection-leg-dir combinations that don't have volumes (to allow for easy averaging) and considered a valid movement. See [`volumes_15min_tmc`](#volumes_15min_tmc)  for more detail on gap filling and [QC Checks](#qc-checks) for more detail on what is a valid movement.
2. Run [`SELECT mioviosion.aggregate_15_min()`](sql/function-aggregate-volumes_15min.sql). This produces 15-minute aggregated segment-level (i.e. ATR) data. A crossover table [`atr_tmc_uid`](#atr_tmc_uid) also populated using this query. This query contains a list of every combination of `volume_15min_uid` and `volume_15min_tmc_uid` since the relationship between the two tables is a many to many relationship.

### C. Refresh reporting views

This produces a lookup table of date-intersection combinations to be used for formal reporting (this filters into various views).

Refresh the `MATERIALIZED VIEW WITH DATA`s in the following order for reporting by running [`SELECT miovision.refresh_views()`](sql/function_refresh_materialized_views.sql). The following views are refreshed:
   * [`miovision.report_dates`](sql/materialized-view-report_dates.sql): This view contains a record for each intersection-date combination in which at least **forty** 15-minute time bins exist. If only limited/peak period data is collected, exceptions should be added into the `exceptions` table.
   * [`miovision.volumes_15min_by_class`](sql/create-view-volumes_15min_by_class.sql): Contains segment level data in 15 minute bins, categorized by cyclists, pedestrians, and vehicles. The data is similar to the `volumes_15min` table except the data is combined into the new classifications.
   * [`miovision.report_volumes_15min`](sql/create-view-report_volumes_15min.sql): Checks if any 15 minute bins are missing from 7:00 and 20:00. If a 15 minute bin is missing and hasn't already been filled, the view will fill it with the monthly average for that intersection-leg-classification-time combination. Data outside those hours are not kept in the view.
   * [`miovision.report_daily`](sql/create-view-report-daily.sql): Aggregates the data further into 3 periods: 14 hours, AM Peak, and PM Peak. The data is also aggregated into EB and WB directions and movements are not kept. Northbound and southbound directions do not appear in this view. 

### D. Produce summarized monthly reporting data

Add the relevant months to the [`VIEW miovision.report_summary`](sql/create-view-report_summary.sql). This view averages the volumes in `report_daily` for each intersection-direction-month-period combination.  Copy over the new month in `report_summmary` to the excel template `Count Data Draft.xlsx`, and add it as a new month in the `tod` excel sheets. Then add the new month and references to the `tod` sheet to the other excel sheets.

The excel spreadsheet rearranges and rounds the data from `report_summary` so that the output is easily readable and compares the data against previous months and the baseline.

### Deleting Data

It is possible to enable a `FOREIGN KEY` relationship to `CASCADE` a delete from a referenced row (an aggregate one in this case) to its referring rows (disaggregate). However not all rows get ultimately processed into aggregate data. In order to simplify the deletion process, `TRIGGER`s have been set up on the less processed datasets to cascade deletion up to processed data. These can be found in [`trigger-delete-volumes.sql`](sql/trigger-delete-volumes.sql). At present, deleting rows in `raw_data` will trigger deleting the resulting rows in `volumes` and then `volumes_15min_mvt` and `volumes_15min`. 0 rows in `volumes_15min_tmc` are deleted through the intermediary lookup [`volumes_tmc_zeroes`](#volumes_tmc_zeroes).

## 8. Filtering and Interpolation (NO LONGER IN USE)

### Filtering

Any 15-minute bin which has fewer than 5 distinct one-minute observations is excluded from 15-minute aggregation.

### Interpolation

In the event there are fewer than 15 minutes of data, interpolation may occur to create the aggregated 15 minute bin in `volumes_15min_tmc`. The interpolation process scales up the average of the observed 1-minute bins to the full 15-minutes. Interpolation only happens for those 15min bins if there are more than 5 distinct 1min bin and less than 15 distinct 1min bin. Note that the process doesn't interpolate if the 1min bin before and after that 15min bin has data. 

```SQL
WHEN B.interpolated = TRUE
THEN SUM(A.volume)*15.0/((EXTRACT(minutes FROM B.end_time - B.start_time)+1)*1.0)
```

Interpolation should only occur when missing 1 minute bins are due to the camera being inoperational rather than no volume being counted.

* If the missing bins are between the earliest and latest recorded bin within that 15-minute period, then it can be assumed the missing bin/bins are due to no observed volumes for those minutes. This can be caught if difference between the start and end time is greater than the number of populated minutes in the bin.
* If there are populated bins in the minute or 2 minute before AND after the 15 minute bin, then it can also be assumed that any missing bins are due to no volume at that minute.

## 9. QC Checks

These are some checks to identify issues.

Compare the `report_daily` view with what is present on the [datalink portal](https://datalink.miovision.com/). The portal breaks down the volumes by day, classification, hour, 15 minute bin and is considered the truth.

### Variance check

```SQL
SELECT intersection_uid, intersection_name, street_main, street_cross, class_type, dir, period_name, min(total_volume) AS min_total_volume, max(total_volume) as max_total_volume
FROM miovision.report_daily
WHERE period_type = 'Jul 2018' AND period_name IN ('AM Peak Period', 'PM Peak Period')
GROUP BY intersection_uid, intersection_name, street_main, street_cross, class_type, dir, period_name
HAVING max(total_volume) / min(total_volume) > 1.5
```

This query searches report daily for dates having a variation of more than 1.5x at the same intersection, direction, class, and period. Usually, the daily variation of volume between days should not be that high, so this query can identify dates and intersections to compare against the datalink.

```SQL
SELECT A.intersection_name, A.class_type, A.dir, A.period_name, A.total_volume/A.total_volume AS monday, B.total_volume/A.total_volume AS tuesday,
C.total_volume/A.total_volume AS wednesday, D.total_volume/A.total_volume AS thursday, E.total_volume/A.total_volume AS friday
From (select intersection_name, intersection_uid,class_type, dir, period_name, total_volume from miovision_new.report_daily WHERE dt='2018-10-15') A
INNER JOIN (select intersection_uid,class_type, dir, period_name, total_volume from miovision_new.report_daily WHERE dt='2018-10-17') E USING (intersection_uid,class_type, dir, period_name)
INNER JOIN (select intersection_uid,class_type, dir, period_name, total_volume from miovision_new.report_daily WHERE dt='2018-10-18') C USING (intersection_uid,class_type, dir, period_name)
INNER JOIN (select intersection_uid,class_type, dir, period_name, total_volume from miovision_new.report_daily WHERE dt='2018-10-19') D USING (intersection_uid,class_type, dir, period_name)
INNER JOIN (select intersection_uid,class_type, dir, period_name, total_volume from miovision_new.report_daily WHERE dt='2018-10-30') B USING (intersection_uid,class_type, dir, period_name)
WHERE period_name<>'14 Hour'
```

This query checks the relative variance of the volumes over the day to make sure that any variance is reflected in other intersection/class's. This is more useful than the other query when the weather changes over the week.

### Invalid Movements

The data also occasionally includes volumes with invalid movements. An example would be a WB thru movement on an EB one-way street such as Adelaide. Run [`find_invalid_movements.sql`](sql/function-find_invalid_movements.sql) to look for invalid volumes that may need to be deleted. This will create a warning if the number of invalid movements is higher than 1000, and that further QC is needed.

## 10. Open Data

For the King Street Transit Pilot, the volume datasets listed below were released. These two datassets are georeferenced by intersection id:

- [King St. Transit Pilot – Detailed Traffic & Pedestrian Volumes](https://open.toronto.ca/dataset/king-st-transit-pilot-detailed-traffic-pedestrian-volumes/) contains 15 minute aggregated [TMC](#turning-movement-counts-tmcs) data collected from Miovision readers during the King Street Pilot. The counts occurred at 31-32 locations at or around the King Street Pilot Area ([SQL](miovision\sql\open_data_views.sql)).
- [King St. Transit Pilot - Traffic & Pedestrian Volumes Summary](https://open.toronto.ca/dataset/king-st-transit-pilot-traffic-pedestrian-volumes-summary/) is a monthly summary of the above data, only including peak period and east-west data ([SQL](miovision\sql\open_data_views.sql)). The data in this dataset goes into the [King Street Pilot Dashboard](https://www.toronto.ca/city-government/planning-development/planning-studies-initiatives/king-street-pilot/data-reports-background-materials/)


## 11. Deprecated Airflow DAGs

<!-- pull_miovision_doc_md -->
### **`pull_miovision`**  
This deprecated Miovisiong DAG (replaced by [`miovision_pull`](api/readme.md#miovision_pull) uses a single BashOperator to run the entire data pull and aggregation in one task.  
The BashOperator runs one task named `pull_miovision` using a bash command that looks something like this bash_command = `'/data/airflow/.../intersection_tmc.py run-api --pull --agg --path /data/airflow/.../config.cfg'`. 
<!-- pull_miovision_doc_md -->

<!-- check_miovision_doc_md -->
### **`check_miovision`**

The `check_miovision` DAG is deprecated by the addition of the [`data_checks` TaskGroup](api/readme.md#data_checks-taskgroup) to the main `miovision_pull` DAG (and `miovision_check` DAG), in particular `miovision_check.check_gaps` which directly replaces `check_miovision.check_miovision`.  
This DAG previously was used to check if any Miovision camera had a gap of at least 4 hours. More information can be found at [this part of the readme.](https://github.com/CityofToronto/bdit_data-sources/tree/miovision_api_bugfix/volumes/miovision#3-finding-gaps-and-malfunctioning-camera)
<!-- check_miovision_doc_md -->