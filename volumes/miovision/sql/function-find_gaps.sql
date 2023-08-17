
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
    --LEFT JOIN miovision_api.volumes AS v USING (intersection_uid, datetime_bin)
    --WHERE v.intersection_uid IS NULL --row does not exist in volumes

    UNION 

    SELECT 
        intersection_uid,
        datetime_bin,
        interval '1 minute' AS gap_adjustment --need to reduce gap by 1 minute for real data
    FROM miovision_api.volumes AS v
    WHERE
        datetime_bin >= start_date
        AND datetime_bin < end_date
),

bin_times AS (
    SELECT 
        intersection_uid,
        datetime_bin AS gap_start,
        LEAD(datetime_bin, 1) OVER (PARTITION BY intersection_uid ORDER BY datetime_bin) AS gap_end,
        --these variables are no longer needed to determine allowable_gap.    
        --CASE
        --    WHEN date_part('isodow', datetime_bin) <= 5 AND holiday.holiday IS NULL THEN False
        --    ELSE True
        --END as weekend,
        --date_part('hour', datetime_bin) AS day_hour,
        LEAD(datetime_bin, 1) OVER (PARTITION BY intersection_uid ORDER BY datetime_bin)
            - datetime_bin != interval '1 minute' AS bin_break,
        LEAD(datetime_bin, 1) OVER (PARTITION BY intersection_uid ORDER BY datetime_bin) 
            - datetime_bin
            - gap_adjustment
        AS bin_gap,
        gap_adjustment
    FROM fluffed_data
    LEFT JOIN ref.holiday ON holiday.dt = datetime_bin::date
    GROUP BY
        intersection_uid, 
        gap_start,
        gap_adjustment
), 

gaps AS (
	-- calculate the start and end times of gaps that are longer than 1 minutes
	INSERT INTO miovision_api.unacceptable_gaps(intersection_uid, gap_start, gap_end, gap_minute, allowed_gap, accept)
	-- Distinct on means multiple gaps during the hour won't be stored
	SELECT DISTINCT ON (intersection_uid, date_part('hour', gap_start))
		intersection_uid,
		gap_start,
		gap_end,
		(date_part('epoch', bin_gap) / 60::integer) AS gap_minute,
		15 AS allowed_gap,
		False AS accept
	FROM bin_times
	WHERE 
		bin_break = True
		AND gap_end IS NOT NULL
		--change from a dynamicly calculated gap to a standard one across all cases
		AND bin_gap >= interval '15 minutes'
	ORDER BY
		intersection_uid,
		date_part('hour', gap_start)
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
