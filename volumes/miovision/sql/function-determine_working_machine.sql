CREATE OR REPLACE FUNCTION miovision_api.determine_working_machine(
	start_date date,
	end_date date)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE tot integer;

BEGIN
WITH ful AS (
SELECT generate_series(start_date::date, end_date::date, interval '1 minute')::timestamp without time zone 
	AS datetime_bin)
, groups AS (
	SELECT
	volume_uid,
	intersection_uid,
    datetime_bin,
	dense_rank() OVER (ORDER BY ful.datetime_bin)
	- dense_rank() OVER (PARTITION BY intersection_uid ORDER BY vol.datetime_bin) AS diff
	FROM ful
	LEFT JOIN miovision_api.volumes_test vol
	USING (datetime_bin)
) , island AS (
SELECT intersection_uid, 
MAX(datetime_bin) - MIN(datetime_bin) + interval '1 minute' AS island_size, 
MIN(datetime_bin) AS time_min, MAX(datetime_bin) AS time_max
FROM groups
GROUP BY intersection_uid, diff
ORDER BY intersection_uid, time_min
) , gap AS (
SELECT
    ROW_NUMBER() OVER(ORDER BY intersection_uid, time_min) AS rn,
	intersection_uid,
	island_size,
    time_min,
    time_max,
    LAG(time_max,1) OVER (PARTITION BY intersection_uid ORDER BY intersection_uid, time_min) AS prev_time_end,
	time_min - LAG(time_max,1) OVER (PARTITION BY intersection_uid ORDER BY intersection_uid, time_min) AS gap_size
FROM island
ORDER BY rn
), missing AS (
SELECT intersection_uid, island_size, time_min, time_max, prev_time_end, gap_size
FROM gap 
WHERE island_size > '23:00:00'::time
OR gap_size > '08:00:00'::time  --this magic number might need to be changed
GROUP BY intersection_uid, island_size, time_min, time_max, prev_time_end, gap_size
	)
SELECT COUNT(intersection_uid) INTO tot
FROM missing;

IF tot > 0 
	THEN RAISE NOTICE 'There''s % gap in the data for all active interesection. A camera might be down.', tot;
	RETURN 1;
ELSE 
	RAISE NOTICE 'All cameras are working fine';	
	RETURN 0;
END IF;

END;

$BODY$;