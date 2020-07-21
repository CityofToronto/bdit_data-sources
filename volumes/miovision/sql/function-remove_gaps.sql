
CREATE OR REPLACE FUNCTION miovision_api.remove_gaps(
	start_date date,
	end_date date)
    RETURNS integer
    LANGUAGE 'plpgsql'

AS $BODY$
BEGIN
	DROP TABLE IF EXISTS gaps;

	CREATE TEMPORARY TABLE gaps (
			intersection_uid integer,
			gap_start timestamp without time zone,
			gap_end timestamp without time zone,
			hourly_bin timestamp without time zone,
			gap_minute integer,
			allowed_gap integer,
			accept boolean);

--FIRST, FIND ALL GAPS AND INSERT INTO TEMP TABLE
WITH ful AS (
SELECT generate_series(start_date::date, end_date::date, interval '1 minute')::timestamp without time zone 
	AS datetime_bin)
, grp AS (
	SELECT
	vol.volume_uid,
	vol.intersection_uid,
    ful.datetime_bin,
	dense_rank() OVER (ORDER BY ful.datetime_bin)
	- dense_rank() OVER (PARTITION BY vol.intersection_uid ORDER BY vol.datetime_bin) AS diff
	FROM ful
	LEFT JOIN miovision_api.volumes vol
	USING (datetime_bin)
) , island AS (
SELECT grp.intersection_uid, 
MAX(datetime_bin) - MIN(datetime_bin) + interval '1 minute' AS island_size, 
MIN(datetime_bin) AS time_min, MAX(datetime_bin) AS time_max
FROM grp
GROUP BY grp.intersection_uid, diff
ORDER BY grp.intersection_uid, time_min
) , gap AS (
SELECT
    ROW_NUMBER() OVER(ORDER BY island.intersection_uid, time_min) AS rn,
	island.intersection_uid,
	island_size,
    time_min,
    time_max,
    LAG(time_max,1) OVER (PARTITION BY island.intersection_uid ORDER BY island.intersection_uid, time_min) AS prev_time_end,
	time_min - LAG(time_max,1) OVER (PARTITION BY island.intersection_uid ORDER BY island.intersection_uid, time_min) AS gap_size
FROM island
ORDER BY rn
)
INSERT INTO gaps(intersection_uid, gap_start, gap_end, gap_minute)
SELECT gap.intersection_uid, gap.prev_time_end AS gap_start, gap.time_min AS gap_end, 
date_part('epoch'::text, gap.gap_size) / 60::integer AS gap_minute
FROM gap 
WHERE gap.gap_size > '00:01:00'::time  --where gap size is greater than 1 minute
GROUP BY gap.intersection_uid, time_min, prev_time_end, gap.gap_size
ORDER BY gap.intersection_uid, gap_start;


--THEN, FIND UNACCEPTABLE GAP AKA THOSE EXCEEDING ALLOWED_GAP
WITH sel AS (
SELECT intersection_uid, gap_start, gap_end, gap_minute, 
DATE_TRUNC('hour', gaps.gap_start) AS hourly_bin,
DATE_TRUNC('hour', gaps.gap_start)::time AS hourly,
CASE WHEN EXTRACT(isodow FROM gaps.gap_start) IN (1,2,3,4,5) THEN 'Weekday'
WHEN EXTRACT(isodow FROM gaps.gap_start) IN (6,7) THEN 'Weekend'
ELSE NULL
END AS day_type
FROM gaps
	)
INSERT INTO gaps(intersection_uid, gap_start, gap_end, hourly_bin, gap_minute, allowed_gap, accept)
SELECT sel.intersection_uid, sel.gap_start, sel.gap_end, sel.hourly_bin,
	sel.gap_minute, lookup.gap_size AS allowed_gap,
CASE WHEN gap_minute < lookup.gap_size THEN TRUE
WHEN gap_minute >= lookup.gap_size THEN FALSE
END AS accept
FROM sel
LEFT JOIN miovision_api.gapsize_lookup lookup
ON sel.intersection_uid = lookup.intersection_uid
AND sel.hourly = lookup.time_bin
AND sel.day_type = lookup.period;

--OR ELSE WE WILL HAVE DUPLICATE ROWS 
DELETE FROM gaps WHERE hourly_bin IS null;

--FIND 1-MIN BIN WITHIN THE UNACCEPTABLE GAPS AND INSERT THAT INTO ANOTHER TABLE
INSERT INTO miovision_api.intersection_outages
WITH un AS 
(
SELECT DISTINCT ON (intersection_uid, hourly_bin) 
	intersection_uid, gap_start, gap_end, gap_minute, allowed_gap
FROM gaps
WHERE accept = FALSE
)
SELECT vol.volume_uid , vol.intersection_uid, vol.datetime_bin, 
vol.classification_uid, vol.leg, vol.movement_uid, vol.volume
FROM un 
LEFT JOIN miovision_api.volumes vol
ON un.intersection_uid = vol.intersection_uid
AND DATE_TRUNC('day', un.gap_start)::date = DATE_TRUNC('day', vol.datetime_bin)::date 
AND EXTRACT(hour FROM un.gap_start) = EXTRACT(hour FROM vol.datetime_bin)
-- to index miovision volumes table (ONLY TIMESTAMP IS INDEXED AND NOT DATE)
WHERE vol.datetime_bin >= start_date::timestamp without time zone
AND vol.datetime_bin < end_date::timestamp without time zone
ORDER BY volume_uid, vol.intersection_uid;

RETURN 1;
END;
$BODY$;