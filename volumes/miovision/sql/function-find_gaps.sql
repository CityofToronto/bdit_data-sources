
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

--get first and last datetime_bins for each intersection if they don't exist
WITH fluffed_data AS (
    SELECT
        i.intersection_uid,
        bins.datetime_bin,
        interval '0 minutes' AS gap_adjustment --don't need to reduce gap width for artificial data
    FROM miovision_api.intersections AS i
    --add artificial data points every hour to enforce hourly outages,
	--including for intersections with no data otherwise.
    CROSS JOIN generate_series(
            start_date::timestamp,
            end_date::timestamp,
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
        AND datetime_bin < end_date
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
		date_part('epoch', bin_gap) / 60::integer AS gap_minute
	FROM bin_times
	WHERE
		bin_break = True
		AND gap_end IS NOT NULL
)

-- summarize gaps into hours containing gaps of 15 minutes or more.
gaps AS (
	INSERT INTO miovision_api.unacceptable_gaps(intersection_uid, gap_start, gap_end, gap_minute, allowed_gap, accept)
	SELECT
		intersection_uid,
		date_trunc('hour', gap_start) AS gap_start,
		date_trunc('hour', gap_start) + interval '1 hour' AS gap_end,
		SUM(gap_minute) AS gap_minute,
		15 AS allowed_gap,
		False AS accept
	FROM all_gaps
    --change from a dynamicly calculated gap to a standard one across all cases
    WHERE gap_minute >= 15	
    GROUP BY
    	intersection_uid,
		date_trunc('hour', gap_start)
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
