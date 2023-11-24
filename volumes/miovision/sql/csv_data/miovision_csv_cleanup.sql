--examine the row counts and extents of all the raw data tables:
SELECT 'raw_data' AS table, MIN(datetime_bin), MAX(datetime_bin), COUNT(DISTINCT datetime_bin::date) AS days_of_data, 'This is the main table for the schema. Keep' AS comment
FROM miovision_csv.raw_data
UNION
SELECT 'raw_data_new', MIN(datetime_bin), MAX(datetime_bin), COUNT(DISTINCT datetime_bin::date) AS days_of_data, 'Two days of data, Drop.' AS comment
FROM miovision_csv.raw_data_new
UNION
SELECT 'raw_data_old' AS table, MIN(datetime_bin), MAX(datetime_bin), COUNT(DISTINCT datetime_bin::date) AS days_of_data, 'Time range overlaps with main table. Drop.' AS comment
FROM miovision_csv.raw_data_old
UNION
SELECT 'raw_data_20180717' AS table, MIN(datetime_bin), MAX(datetime_bin), COUNT(DISTINCT datetime_bin::date) AS days_of_data, 'Ten days of data. Drop.' AS comment
FROM miovision_csv.raw_data_20180717
UNION
SELECT 'raw_data2020' AS table, MIN(datetime_bin), MAX(datetime_bin), COUNT(DISTINCT datetime_bin::date) AS days_of_data, 'Empty table, drop.' AS comment 
FROM miovision_csv.raw_data2020
UNION
SELECT 'volumes' AS table, MIN(datetime_bin), MAX(datetime_bin), COUNT(DISTINCT datetime_bin::date) AS days_of_data, 'This is the real raw table!' AS comment 
FROM miovision_csv.volumes

| "table"             | "min"                    | "max"                    | "days_of_data" | "comment"                                    |
|---------------------|--------------------------|--------------------------|----------------|----------------------------------------------|
| "raw_data_old"      | "2017-10-03 17:16:00+00" | "2018-03-10 04:59:00+00" | 61             | "Time range overlaps with main table. Drop." |
| "raw_data_20180717" | "2018-06-18 11:00:00+00" | "2018-06-28 01:59:00+00" | 10             | "Ten days of data. Drop."                    |
| "raw_data2020"      | 0                        | "Empty table, drop."     |                |                                              |
| "raw_data"          | "2017-10-03 17:16:00+00" | "2018-07-27 22:59:00+00" | 83             | "This is the main table for the schema. Keep"|
| "raw_data_new"      | "2018-03-09 05:00:00+00" | "2018-03-10 04:59:00+00" | 2              | "Two days of data, Drop."                    |
| "volumes"           | "2017-10-03 13:16:00+00" | "2018-08-24 21:59:00+00" | 91             | "This is the real raw table!"                |

--drop 2020 functions:
DROP FUNCTION miovision_csv.aggregate_15_min_2020;
DROP FUNCTION miovision_csv.find_gaps_2020;
DROP FUNCTION miovision_csv.aggregate_15_min_tmc_2020;
DROP FUNCTION miovision_csv.insert_volumes_2020(); --table volumes_2020 is depenendent on this function

--drop "2020" tables. 
DROP TABLE miovision_csv.raw_data2020;
DROP TABLE miovision_csv.volumes_2020;
DROP TABLE miovision_csv.volumes2020_15min;
DROP TABLE miovision_csv.volumes2020_15min_tmc;
DROP TABLE miovision_csv.volumes2020_tmc_atr_xover;
DROP TABLE miovision_csv.unacceptable_gaps_2020;

--this table can be truncated because the newer process used a trigger to instead bypass raw_data and insert data into miovision_csv.volumes. 
--the 2GB of data in this table is duplicate with that in miovision_csv.volumes. 
TRUNCATE TABLE miovision_csv.raw_data;

--add better comments on schema, comments on tables 
COMMENT ON SCHEMA miovision_csv IS 'Older Miovision data from 2017/2018. Data format is more temporally sparse than newer miovision_api schema but used a human-augmented process for higher quality counts.';

--this table has no dependents and appears to be superseeded by `report_dates`.
DROP TABLE miovision_csv.report_dates_old;

--this small lookup table has no dependents and no references in miovision_csv
DROP TABLE miovision_csv.periods;

--most tables are owned by aakash, change: 
ALTER TABLE miovision_csv.baseline_dates OWNER TO miovision_admins;
ALTER TABLE miovision_csv.baselines OWNER TO miovision_admins;
ALTER TABLE miovision_csv.baselines_agg OWNER TO miovision_admins;
ALTER TABLE miovision_csv.class_types OWNER TO miovision_admins;
ALTER TABLE miovision_csv.classifications OWNER TO miovision_admins;
ALTER TABLE miovision_csv.exceptions OWNER TO miovision_admins;
ALTER TABLE miovision_csv.intersection_movements OWNER TO miovision_admins;
ALTER TABLE miovision_csv.intersections OWNER TO miovision_admins;
ALTER TABLE miovision_csv.movement_map OWNER TO miovision_admins;
ALTER TABLE miovision_csv.movements OWNER TO miovision_admins;
ALTER TABLE miovision_csv.periods OWNER TO miovision_admins;
ALTER TABLE miovision_csv.raw_data OWNER TO miovision_admins;
ALTER TABLE miovision_csv.raw_data2020 OWNER TO miovision_admins;
ALTER TABLE miovision_csv.raw_data_20180717 OWNER TO miovision_admins;
ALTER TABLE miovision_csv.raw_data_new OWNER TO miovision_admins;
ALTER TABLE miovision_csv.raw_data_old OWNER TO miovision_admins;
ALTER TABLE miovision_csv.report_dates OWNER TO miovision_admins;
ALTER TABLE miovision_csv.report_dates_old OWNER TO miovision_admins;
ALTER TABLE miovision_csv.unacceptable_gaps_2020 OWNER TO miovision_admins;
ALTER TABLE miovision_csv.volumes OWNER TO miovision_admins;
ALTER TABLE miovision_csv.volumes2020_15min OWNER TO miovision_admins;
ALTER TABLE miovision_csv.volumes2020_15min_tmc OWNER TO miovision_admins;
ALTER TABLE miovision_csv.volumes2020_tmc_atr_xover OWNER TO miovision_admins;
ALTER TABLE miovision_csv.volumes_15min OWNER TO miovision_admins;
ALTER TABLE miovision_csv.volumes_15min_tmc OWNER TO miovision_admins;
ALTER TABLE miovision_csv.volumes_2020 OWNER TO miovision_admins;
ALTER TABLE miovision_csv.volumes_tmc_atr_xover OWNER TO miovision_admins;
ALTER TABLE miovision_csv.volumes_tmc_zeroes OWNER TO miovision_admins;

--this function has no dependents and is not referenced in github. 
DROP FUNCTION miovision_csv.aggregate_15_min_tmc_new;