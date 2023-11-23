--function to identify gaps in miovision_api.volumes data and insert into
-- miovision_api.unacceptable_gaps table. gap_tolerance set using 60 day 
-- lookback avg volumes and thresholds defined in gapsize_lookup. 

CREATE OR REPLACE FUNCTION miovision_api.find_gaps(
    start_date date)
RETURNS integer
LANGUAGE 'plpgsql'

COST 100
VOLATILE 
AS $BODY$

DECLARE tot_gaps integer;

BEGIN

--hourly sum for last 60 days to use in gapsize_lookup. 
WITH mio AS (
    SELECT
        intersection_uid,
        date_trunc('day', datetime_bin) AS dt,
        date_part('hour', datetime_bin) AS time_bin,   
        SUM(volume) AS vol
    FROM miovision_api.volumes
    WHERE
        datetime_bin > start_date - interval '60 days'
        AND datetime_bin < start_date
    GROUP BY
        intersection_uid,
        dt,
        time_bin
),

--avg of hourly volume by intersection and hour of day to determine gap_tolerance
gapsize_lookup AS (
    SELECT
        intersection_uid,
        time_bin,
        CASE
            WHEN date_part('isodow', dt) <= 5 AND hol.holiday IS NULL THEN False
            ELSE True
        END as weekend,
        AVG(vol) AS avg_vol,
        CASE
            WHEN AVG(vol) < 100::numeric THEN 20
            WHEN AVG(vol) >= 100::numeric AND AVG(vol) < 500::numeric THEN 15
            WHEN AVG(vol) >= 500::numeric AND AVG(vol) < 1500::numeric THEN 10
            WHEN AVG(vol) > 1500::numeric THEN 5
            ELSE NULL::integer
        END AS gap_tolerance
    FROM mio
    LEFT JOIN ref.holiday AS hol USING (dt)
    GROUP BY
        intersection_uid,
        weekend,
        time_bin
), 

--now find the gaps
fluffed_data AS (
    SELECT
        i.intersection_uid,
        bins.datetime_bin,
        interval '0 minutes' AS gap_adjustment --don't need to reduce gap width for artificial data
    FROM miovision_api.intersections AS i
    --add artificial data points every hour to enforce hourly outages,
    --including for intersections with no data otherwise.
    CROSS JOIN generate_series(
            start_date::timestamp, --start_date
            start_date::timestamp + interval '1 day',
            interval '1 hour'
    ) AS bins(datetime_bin)
    
    --group by in next step takes care of duplicates
    UNION ALL

    SELECT DISTINCT
        intersection_uid,
        datetime_bin,
        --need to reduce gap length by 1 minute for real data since that minute contains data
        interval '1 minute' AS gap_adjustment
    FROM miovision_api.volumes
    WHERE
        datetime_bin >= start_date
        AND datetime_bin < start_date + interval '1 day'
),

--looks at sequential bins to identify breaks larger than 1 minute.
bin_times AS (
    SELECT 
        intersection_uid,
		--sum works because between 0 and 1, we want 1 (implies real data)
        datetime_bin + SUM(gap_adjustment) AS gap_start,
        date_part('hour', datetime_bin) AS time_bin,
        LEAD(datetime_bin, 1) OVER (PARTITION BY intersection_uid ORDER BY datetime_bin) AS gap_end,
        LEAD(datetime_bin, 1) OVER (PARTITION BY intersection_uid ORDER BY datetime_bin)
            - datetime_bin > interval '1 minute' AS bin_break, --True means gap between bins is larger than 1 minute
        CASE
            WHEN date_part('isodow', datetime_bin) <= 5
                AND hol.holiday IS NULL THEN False
            ELSE True
        END as weekend
    FROM fluffed_data
    LEFT JOIN ref.holiday AS hol ON hol.dt = fluffed_data.datetime_bin::date
    GROUP BY
        intersection_uid, 
        datetime_bin,
        weekend
),

gaps AS (
	--find gaps of any size before summarizing (in case we want to use this raw output for something else)
	INSERT INTO miovision_api.unacceptable_gaps(
		intersection_uid, gap_start, gap_end, gap_minute, allowed_gap, accept
	)
	SELECT
		bt.intersection_uid,
		bt.gap_start,
		bt.gap_end,
		gm.gap_minute,
		gl.gap_tolerance AS allowed_gap,
		False AS accept
	FROM bin_times AS bt
	LEFT JOIN gapsize_lookup AS gl USING (intersection_uid, time_bin, weekend),
	LATERAL (
		SELECT date_part('epoch', gap_end - gap_start) / 60::integer AS gap_minute
	) AS gm
	WHERE
		gm.gap_minute >= gl.gap_tolerance    
		AND bt.bin_break = True
		AND bt.gap_end IS NOT NULL
	ON CONFLICT DO NOTHING
	RETURNING *
)

-- FOR NOTICE PURPOSES ONLY
SELECT COUNT(*) INTO tot_gaps
FROM gaps;

RAISE NOTICE 'Found a total of % gaps that are unacceptable', tot_gaps;

RETURN 1;
END;
$BODY$;
