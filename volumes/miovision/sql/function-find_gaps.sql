
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
WITH wkdy_lookup(period, isodow) AS (
         VALUES ('Weekday'::text,'[1,6)'::int4range), ('Weekend'::text,'[6,8)'::int4range)
), mio AS (
 SELECT volumes.intersection_uid,
    datetime_bin(volumes.datetime_bin, 60) AS hourly_bin,
    sum(volumes.volume) AS vol
   FROM miovision_api.volumes
  WHERE volumes.datetime_bin >= GREATEST(start_date::timestamp without time zone - '60 days'::interval, '2019-01-01 00:00:00') AND
        volumes.datetime_bin < GREATEST(start_date::timestamp without time zone, '2019-03-01 00:00:00')
  GROUP BY volumes.intersection_uid, (datetime_bin(volumes.datetime_bin, 60))
), gapsize_lookup AS (
 SELECT mio.intersection_uid,
    d.period,
    mio.hourly_bin::time without time zone AS time_bin,
    avg(mio.vol) AS avg_vol,
        CASE
            WHEN avg(mio.vol) < 100::numeric THEN 20
            WHEN avg(mio.vol) >= 100::numeric AND avg(mio.vol) < 500::numeric THEN 15
            WHEN avg(mio.vol) >= 500::numeric AND avg(mio.vol) < 1500::numeric THEN 10
            WHEN avg(mio.vol) > 1500::numeric THEN 5
            ELSE NULL::integer
        END AS gap_size
   FROM mio
     CROSS JOIN wkdy_lookup d
  WHERE date_part('isodow'::text, mio.hourly_bin)::integer <@ d.isodow
  GROUP BY mio.intersection_uid, d.period, (mio.hourly_bin::time without time zone)
), ful AS (
	SELECT generate_series(start_date, end_date, interval '1 minute')::timestamp without time zone 
		AS datetime_bin
), grp AS (
	SELECT
	vol.volume_uid,
	vol.intersection_uid,
    ful.datetime_bin,
	dense_rank() OVER (ORDER BY ful.datetime_bin)
	- dense_rank() OVER (PARTITION BY vol.intersection_uid ORDER BY vol.datetime_bin) AS diff
	FROM ful
	LEFT JOIN miovision_api.volumes vol
	USING (datetime_bin)
), island AS (
	SELECT grp.intersection_uid, 
	MAX(datetime_bin) - MIN(datetime_bin) + interval '1 minute' AS island_size, 
	MIN(datetime_bin) AS time_min, MAX(datetime_bin) AS time_max
	FROM grp
	GROUP BY grp.intersection_uid, diff
	ORDER BY grp.intersection_uid, time_min
), gap AS (
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
), sel AS (
	SELECT gap.intersection_uid, gap.prev_time_end AS gap_start, gap.time_min AS gap_end, 
	date_part('epoch'::text, gap.gap_size) / 60::integer AS gap_minute
	FROM gap 
	WHERE gap.gap_size > '00:01:00'::time  --where gap size is greater than 1 minute
	GROUP BY gap.intersection_uid, time_min, prev_time_end, gap.gap_size
	ORDER BY gap.intersection_uid, gap_start
), acceptable AS (
	-- THEN, MATCH IT TO THE LOOKUP TABLE TO CHECK IF ACCEPTABLE
	SELECT sel.intersection_uid, sel.gap_start, sel.gap_end,
		sel.gap_minute, gapsize_lookup.gap_size AS allowed_gap,
	CASE WHEN gap_minute < gapsize_lookup.gap_size THEN TRUE
	WHEN gap_minute >= gapsize_lookup.gap_size THEN FALSE
	END AS accept
	FROM sel 
	LEFT JOIN gapsize_lookup
	ON sel.intersection_uid = gapsize_lookup.intersection_uid
	AND DATE_TRUNC('hour', sel.gap_start)::time = gapsize_lookup.time_bin
	AND (CASE WHEN EXTRACT(isodow FROM sel.gap_start) IN (1,2,3,4,5) THEN 'Weekday'
	WHEN EXTRACT(isodow FROM sel.gap_start) IN (6,7) THEN 'Weekend'
	ELSE NULL END)  = gapsize_lookup.period
), fail AS (
	-- INSERT INTO THE TABLE
	-- DISTINCT ON cause there might be more than 1 gap happening in the same hour for the same intersection
	INSERT INTO miovision_api.unacceptable_gaps(intersection_uid, gap_start, gap_end, gap_minute, allowed_gap, accept)
	SELECT DISTINCT ON (intersection_uid, DATE_TRUNC('hour', acceptable.gap_start)) *
	FROM acceptable
	WHERE accept = false
	ORDER BY DATE_TRUNC('hour', acceptable.gap_start), intersection_uid
	RETURNING *
)
-- FOR NOTICE PURPOSES ONLY
SELECT COUNT(*) INTO tot_gaps
FROM fail ;

RAISE NOTICE 'Found a total of % (hours) gaps that are unacceptable' , tot_gaps;

RETURN 1;
END;
$BODY$;
