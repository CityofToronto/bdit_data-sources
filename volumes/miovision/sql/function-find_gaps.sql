
CREATE OR REPLACE FUNCTION miovision_api.find_gaps(
	start_date date)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE tot_gaps integer;

BEGIN

--hourly sum for last 60 days
WITH mio AS (
    SELECT
        intersection_uid,
        datetime_bin::date AS date,
        date_part('hour', datetime_bin) AS time_bin,   
        SUM(volume) AS vol
    FROM miovision_api.volumes
    WHERE
        datetime_bin > start_date - interval '60 days'
        AND datetime_bin < start_date
    GROUP BY
        intersection_uid,
        date,
        time_bin
),

--avg of hourly volume by intersection and hour of day. 
gapsize_lookup AS (
    SELECT
        mio.intersection_uid,
        mio.time_bin,
        CASE
            WHEN date_part('isodow', mio.date) <= 5 AND hol.holiday IS NULL THEN False
            ELSE True
        END as weekend,
        AVG(mio.vol) AS avg_vol,
        CASE
            WHEN AVG(mio.vol) < 100::numeric THEN 20
            WHEN AVG(mio.vol) >= 100::numeric AND AVG(mio.vol) < 500::numeric THEN 15
            WHEN AVG(mio.vol) >= 500::numeric AND AVG(mio.vol) < 1500::numeric THEN 10
            WHEN AVG(mio.vol) > 1500::numeric THEN 5
            ELSE NULL::integer
        END AS gap_tolerance
    FROM mio
    LEFT JOIN ref.holiday AS hol ON hol.dt = mio.date
    GROUP BY
        mio.intersection_uid,
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
        datetime_bin AS gap_start,
        LEAD(datetime_bin, 1) OVER (PARTITION BY intersection_uid ORDER BY datetime_bin) AS gap_end,
        LEAD(datetime_bin, 1) OVER (PARTITION BY intersection_uid ORDER BY datetime_bin)
            - datetime_bin > interval '1 minute' AS bin_break, --True means gap between bins is larger than 1 minute
        LEAD(datetime_bin, 1) OVER (PARTITION BY intersection_uid ORDER BY datetime_bin) 
            - datetime_bin
            - SUM(gap_adjustment) --sum works because between 0 and 1, we want 1 (implies real data)
        AS bin_gap
    FROM fluffed_data
    GROUP BY
        intersection_uid, 
        datetime_bin
),

--find gaps of any size before summarizing to hourly (in case we want to use this raw output for something else)
all_gaps AS (
	SELECT
		intersection_uid,
		gap_start,
		gap_end,
		date_part('epoch', bin_gap) / 60::integer AS gap_minute,
        date_part('hour', gap_start) AS time_bin, 
        CASE
            WHEN date_part('isodow', gap_start) <= 5 AND hol.holiday IS NULL THEN False
            ELSE True
        END as weekend
	FROM bin_times
    LEFT JOIN ref.holiday AS hol ON hol.dt = bin_times.gap_start::date
	WHERE
		bin_break = True
		AND gap_end IS NOT NULL
),

-- summarize gaps into hours containing gaps of 15 minutes or more.
gaps AS (
	INSERT INTO miovision_api.unacceptable_gaps(intersection_uid, gap_start, gap_end, gap_minute, allowed_gap, accept)
	SELECT
		intersection_uid,
		date_trunc('hour', gap_start) AS gap_start,
		date_trunc('hour', gap_start) + interval '1 hour' AS gap_end,
		SUM(gap_minute) AS gap_minute,
		gap_tolerance AS allowed_gap,
		False AS accept
	FROM all_gaps AS ag
    LEFT JOIN gapsize_lookup AS gl USING (intersection_uid, time_bin, weekend)
    --change from a dynamicly calculated gap to a standard one across all cases
    WHERE gap_minute >= gap_tolerance	
    GROUP BY
    	intersection_uid,
		date_trunc('hour', gap_start),
        gap_tolerance
	ON CONFLICT DO NOTHING
	RETURNING *
)

-- FOR NOTICE PURPOSES ONLY
SELECT COUNT(*) INTO tot_gaps
FROM gaps;

RAISE NOTICE 'Found a total of % (hours) gaps that are unacceptable', tot_gaps;

RETURN 1;
END;
$BODY$;
