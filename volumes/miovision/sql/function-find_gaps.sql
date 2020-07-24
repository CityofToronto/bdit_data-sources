
CREATE OR REPLACE FUNCTION miovision_api.find_gaps(
	start_date date,
	end_date date)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE tot_gaps integer;

BEGIN

-- FIRST, FIND ALL GAPS
WITH ful AS (
SELECT generate_series('2020-07-15'::date, '2020-07-16'::date, interval '1 minute')::timestamp without time zone 
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
) , sel AS
(
SELECT gap.intersection_uid, gap.prev_time_end AS gap_start, gap.time_min AS gap_end, 
date_part('epoch'::text, gap.gap_size) / 60::integer AS gap_minute
FROM gap 
WHERE gap.gap_size > '00:01:00'::time  --where gap size is greater than 1 minute
GROUP BY gap.intersection_uid, time_min, prev_time_end, gap.gap_size
ORDER BY gap.intersection_uid, gap_start
) , acceptable AS (
-- THEN, MATCH IT TO THE LOOKUP TABLE TO CHECK IF ACCEPTABLE
SELECT sel.intersection_uid, sel.gap_start, sel.gap_end,
	sel.gap_minute, lookup.gap_size AS allowed_gap,
CASE WHEN gap_minute < lookup.gap_size THEN TRUE
WHEN gap_minute >= lookup.gap_size THEN FALSE
END AS accept
FROM sel 
LEFT JOIN miovision_api.gapsize_lookup lookup
ON sel.intersection_uid = lookup.intersection_uid
AND DATE_TRUNC('hour', sel.gap_start)::time = lookup.time_bin
AND (CASE WHEN EXTRACT(isodow FROM sel.gap_start) IN (1,2,3,4,5) THEN 'Weekday'
WHEN EXTRACT(isodow FROM sel.gap_start) IN (6,7) THEN 'Weekend'
ELSE NULL END)  = lookup.period
) , fail AS (
-- INSERT INTO THE TABLE
-- DISTINCT ON cause there might be more than 1 gap happening in the same hour for the same intersection
INSERT INTO miovision_api.unacceptable_gaps(intersection_uid, gap_start, gap_end, gap_minute, allowed_gap, accept)
SELECT DISTINCT ON (intersection_uid, DATE_TRUNC('hour', acceptable.gap_start)) *
FROM acceptable
WHERE accept = false
ORDER BY intersection_uid, DATE_TRUNC('hour', acceptable.gap_start)
RETURNING *
	)
-- FOR NOTICE PURPOSES ONLY
SELECT COUNT(*) INTO tot_gaps
FROM fail ;

RAISE NOTICE 'Found a total of % (hours) gaps that are unacceptable' , tot_gaps;

RETURN 1;
END;
$BODY$;
