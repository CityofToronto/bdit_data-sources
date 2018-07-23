DROP TABLE IF EXISTS volume_new;
CREATE TEMPORARY TABLE volume_new
(
  volume_uid integer,
  intersection_uid integer,
  datetime_bin timestamp without time zone,
  classification_uid integer,
  leg text,
  movement_uid integer,
  volume integer,
  volume_15min_tmc_uid integer
);

-- Creates a temporary table with mostly the exact same structure as the miovision.volumes table. 

INSERT INTO volume_new (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)
SELECT B.intersection_uid, (A.datetime_bin AT TIME ZONE 'America/Toronto') AS datetime_bin, C.classification_uid, A.entry_dir_name as leg, D.movement_uid, A.volume
FROM miovision.raw_data_20180717 A
--Replace the table with the current new data table
INNER JOIN miovision.intersections B ON regexp_replace(A.study_name,'Yong\M','Yonge') = B.intersection_name
INNER JOIN miovision.movements D USING (movement)
INNER JOIN miovision.classifications C USING (classification)
ORDER BY (A.datetime_bin AT TIME ZONE 'America/Toronto'), B.intersection_uid, C.classification_uid, A.entry_name, D.movement_uid;

INSERT INTO miovision.volumes (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume, volume_15min_tmc_uid)
SELECT intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume, volume_15min_tmc_uid FROM volume_new;

--Inserts the temporary table to miovision.volumes. 

DROP TABLE IF EXISTS bins;
	CREATE TEMPORARY TABLE bins (
		intersection_uid integer,
		datetime_bin timestamp without time zone,
		avail_minutes integer,
		total_volume bigint,
		start_time timestamp without time zone,
		end_time timestamp without time zone,
		interpolated boolean);

INSERT INTO bins
SELECT 	intersection_uid, 
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) AS datetime_bin,
		COUNT(DISTINCT A.datetime_bin) AS avail_minutes,
		SUM(A.volume) AS total_volume,
		MIN(A.datetime_bin) AS start_time,
		MAX(A.datetime_bin) AS end_time,
		NULL as interpolated
FROM volume_new A

--Uses the temporary volume table instead of the miovision.volumes table.

GROUP BY intersection_uid, TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900);

-- REMOVE ALL 15-minute bins where 5 or less of the 1-minute bins are populated with volume_new
DELETE FROM bins WHERE avail_minutes <= 5;

-- IF # of populated 1-minute bins exceeds difference between start and end time, assume no interpolation needed
UPDATE bins SET interpolated = FALSE WHERE (EXTRACT(minutes FROM end_time - start_time)+1) > avail_minutes AND avail_minutes < 15;

-- IF one of two 1-minute time bins BEFORE and AFTER 15-minute bin are populated, assume no interpolation needed
UPDATE bins A 
SET interpolated = FALSE
FROM (SELECT intersection_uid, datetime_bin, SUM(volume) AS total_volume FROM volume_new GROUP BY intersection_uid, datetime_bin) B
INNER JOIN (SELECT intersection_uid, datetime_bin, SUM(volume) AS total_volume FROM volume_new GROUP BY intersection_uid, datetime_bin) C ON B.intersection_uid = C.intersection_uid
WHERE 	A.interpolated IS NULL 
		AND A.avail_minutes < 15 
		AND A.intersection_uid = B.intersection_uid 
		AND A.intersection_uid = C.intersection_uid 
		AND (B.datetime_bin >= (A.datetime_bin + INTERVAL '15 minutes') AND B.datetime_bin <= (A.datetime_bin + INTERVAL '16 minutes')) 
		AND (C.datetime_bin <= (A.datetime_bin - INTERVAL '1 minute') AND C.datetime_bin >= A.datetime_bin - (INTERVAL '2 minutes'));

-- ASSUME for all other 15-minute bins with missing data, interpolation needed due to missing video
UPDATE bins SET interpolated = TRUE WHERE interpolated IS NULL AND avail_minutes < 15;

-- FOR 15-minute bins with interpolation needed, IF missing data at end of 15-minute period, SET end_time = end_time - 1 minute to account for potential partial count
UPDATE bins SET end_time = end_time - INTERVAL '1 minute' WHERE interpolated = TRUE AND datetime_bin = start_time;

-- FOR 15-minute bins with interpolation needed, IF missing data at start of 15-minute period, SET start_time = start_time + 1 minute to account for potential partial count
UPDATE bins SET start_time = start_time + INTERVAL '1 minute' WHERE interpolated = TRUE AND datetime_bin + INTERVAL '14 minutes' = end_time;

-- INSERT INTO volumes_15min_tmc, with interpolated volumes
WITH aggregate_insert AS(
INSERT INTO miovision.volumes_15min_tmc(intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)
SELECT 	A.intersection_uid,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) AS datetime_bin,
		A.classification_uid,
		A.leg,
		A.movement_uid,
		CASE WHEN B.interpolated = TRUE THEN SUM(A.volume)*15.0/((EXTRACT(minutes FROM B.end_time - B.start_time)+1)*1.0) ELSE SUM(A.volume) END AS volume
FROM volume_new A
INNER JOIN bins B USING (intersection_uid)
WHERE B.start_time <= A.datetime_bin AND B.end_time >= A.datetime_bin 
GROUP BY A.intersection_uid, TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900), A.classification_uid, A.leg, A.movement_uid, B.interpolated, (EXTRACT(minutes FROM B.end_time - B.start_time)+1)
RETURNING volume_15min_tmc_uid, datetime_bin, classification_uid, leg, movement_uid
	)
UPDATE volume_new a
SET volume_15min_tmc_uid = b.volume_15min_tmc_uid
FROM aggregate_insert b
WHERE a.datetime_bin >= b.datetime_bin AND a.datetime_bin < b.datetime_bin + INTERVAL '15 minutes'
AND a.classification_uid  = b.classification_uid 
AND a.leg = b.leg
AND a.movement_uid = b.movement_uid;
