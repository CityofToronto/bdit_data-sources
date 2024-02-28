CREATE OR REPLACE FUNCTION miovision_api.determine_working_machine(
	start_date date,
	end_date date)
    RETURNS TABLE(intersection_uid integer,	gap_start timestamp without time zone,gap_end timestamp without time zone,gap_size interval)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE tot integer;

BEGIN
DROP TABLE IF EXISTS mis;

CREATE TEMPORARY TABLE mis (
		intersection_uid integer,
		gap_start timestamp without time zone,
		gap_end timestamp without time zone,
		gap_size interval);

WITH ful AS (
	SELECT generate_series(start_date::date, end_date::date, interval '1 minute')::timestamp without time zone 
		AS datetime_bin
) , grp AS (
	SELECT
	vol.volume_uid,
	vol.intersection_uid,
    ful.datetime_bin,
	dense_rank() OVER (ORDER BY ful.datetime_bin)
	- dense_rank() OVER (PARTITION BY vol.intersection_uid ORDER BY vol.datetime_bin) AS diff
	FROM ful
	LEFT JOIN (SELECT *
		       FROM miovision_api.volumes
		       WHERE datetime_bin >= start_date::date AND datetime_bin <= end_date::date) vol
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
INSERT INTO mis
SELECT gap.intersection_uid, prev_time_end AS gap_start, time_min AS gap_end, gap.gap_size
FROM gap 
WHERE gap.gap_size > '04:00:00'::time  --where gap size is greater than 4 hours
OR (time_min = end_date
AND gap.gap_size >  '01:00:00'::time ) -- or when there is no data after 23:00:00
GROUP BY gap.intersection_uid, time_min, prev_time_end, gap.gap_size
ORDER BY gap.intersection_uid, gap_start;

SELECT COUNT(DISTINCT(mis.intersection_uid)) FROM mis INTO tot;

IF tot > 0 
	THEN RAISE NOTICE 'There''s % gap in the data for all active intersections. A camera might be down.', tot;
ELSE 
	RAISE NOTICE 'All cameras are working fine';
END IF;

RETURN QUERY
SELECT mis.intersection_uid, mis.gap_start, mis.gap_end, mis.gap_size
FROM mis;

END;

$BODY$;

COMMENT ON FUNCTION miovision_api.determine_working_machine(date, date) IS
'''Function no longer in use. Previously used in `check_miovision` DAG to determine if any cameras
had gaps larger than 4 hours. See: `miovision_check` DAG `check_gaps` task for new implementation.''';
