--function to identify gaps in miovision_api.volumes data and insert into
-- miovision_api.unacceptable_gaps table. gap_tolerance set using 60 day 
-- lookback avg volumes and thresholds defined in gapsize_lookup. 

DROP FUNCTION IF EXISTS miovision_api.find_gaps;
CREATE OR REPLACE FUNCTION miovision_api.find_gaps(
    start_date timestamp)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE 
AS $BODY$

DECLARE tot_gaps integer;

BEGIN
    
    --add to gapsize lookup table for this date.
    PERFORM miovision_api.gapsize_lookup_insert(start_date);
    
    --clear table before inserting
    DELETE FROM miovision_api.unacceptable_gaps WHERE dt = start_date::date;
    
    --find only intersections active today
    WITH daily_intersections AS (
        SELECT DISTINCT v.intersection_uid
        FROM miovision_api.volumes AS v
        INNER JOIN miovision_api.intersections AS i USING (intersection_uid)
        WHERE
            v.datetime_bin >= start_date
            AND v.datetime_bin < start_date + interval '1 day'
            AND v.datetime_bin >= i.date_installed
            AND (
                v.datetime_bin < i.date_decommissioned
                OR i.date_decommissioned IS NULL
            )
    ),

    --combine the artificial and actual datetime_bins. 
    fluffed_data AS (
        --add the start and end of the day interval for each active intersection
        --to make sure the gaps are not open ended. 
        SELECT
            i.intersection_uid,
            bins.datetime_bin,
            interval '0 minutes' AS gap_adjustment --don't need to reduce gap width for artificial data
        FROM daily_intersections AS i
        --add artificial data points at start and end of day to find gaps overlapping start/end.
        CROSS JOIN (
            VALUES
                --catch gaps overlapping days
                (start_date - interval '15 minutes'),
                (start_date + interval '1 day')
        ) AS bins(datetime_bin)

        --group by in next step takes care of duplicates
        UNION ALL

        --the distinct datetime bins which did appear in the day's data. 
        SELECT DISTINCT
            intersection_uid,
            datetime_bin,
            --need to reduce gap length by 1 minute for real data since that minute contains data
            interval '1 minute' AS gap_adjustment
        FROM miovision_api.volumes
        WHERE
            datetime_bin >= start_date - interval '15 minutes'
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
            --weekend is needed to determine which gapsize_lookup to use. 
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
        --find gaps longer than the threshold from gapsize_lookup and cross join with
        --generate_series output to enable easily joining with 15 minute datetime_bins.
        INSERT INTO miovision_api.unacceptable_gaps(
            dt, intersection_uid, gap_start, gap_end, gap_minutes_total,
            allowable_total_gap_threshold, datetime_bin, gap_minutes_15min
        )
        SELECT
            start_date AS dt,
            bt.intersection_uid,
            bt.gap_start,
            bt.gap_end,
            gm.gap_minutes_total,
            gl.gap_tolerance AS allowable_total_gap_threshold,
            bins.datetime_bin,		
            round(gm.gap_minutes_15min) AS gap_minutes_15min
        FROM bin_times AS bt
        --match gaps to the 15 minute bins they intersect
        JOIN generate_series(
                --catch gaps overlapping days
                start_date - interval '15 minutes',
                start_date + interval '1 day',
                interval '15 minutes'
        ) AS bins(datetime_bin) ON
            bins.datetime_bin >= datetime_bin(bt.gap_start, 15)
            AND bins.datetime_bin < datetime_bin_ceil(bt.gap_end, 15)
        --find the acceptable gap size based on historical lookback.
        JOIN miovision_api.gapsize_lookup AS gl ON
            gl.intersection_uid = bt.intersection_uid
            AND gl.hour_bin = date_part('hour', bins.datetime_bin)
            AND gl.dt = start_date::date
            --this contains the gap threshold for all modes combined
            AND gl.classification_uid IS NULL,
        LATERAL (
            SELECT
                date_part('epoch', gap_end - gap_start) / 60::integer AS gap_minutes_total,
                --find the gap minutes which occured only during the 15 minute bin (bins.datetime_bin)
                EXTRACT (EPOCH FROM
                     LEAST(bt.gap_end, bins.datetime_bin + interval '15 minutes')
                     - GREATEST(bt.gap_start, bins.datetime_bin)
                    ) / 60 AS gap_minutes_15min
        ) AS gm
        WHERE
            gm.gap_minutes_total >= gl.gap_tolerance
            AND bt.bin_break = True            
            AND bt.gap_end IS NOT NULL
            --gap entirely outside of todays range
            AND NOT(bt.gap_end <= start_date)
        ORDER BY
            bt.intersection_uid,
            bins.datetime_bin
        ON CONFLICT (intersection_uid, datetime_bin)
        DO NOTHING
        RETURNING *
    )

    -- FOR NOTICE PURPOSES ONLY
    SELECT COUNT(*) INTO tot_gaps
    FROM gaps;

    RAISE NOTICE 'Found a total of % gaps that are unacceptable', tot_gaps;

END;
$BODY$;