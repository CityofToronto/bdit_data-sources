--function to identify gaps in miovision_api.volumes data and insert into
-- miovision_api.unacceptable_gaps table. gap_tolerance set using 60 day 
-- lookback avg volumes and thresholds defined in gapsize_lookup. 

CREATE OR REPLACE FUNCTION miovision_api.find_gaps(
    start_date timestamp,
    end_date timestamp,
    intersections integer [] DEFAULT ARRAY[]::integer []
)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE
AS $BODY$

DECLARE
    target_intersections integer [] = miovision_api.get_intersections_uids(intersections);
    tot_gaps integer;

BEGIN
    
    --add to gapsize lookup table for this date/intersection.
    PERFORM miovision_api.gapsize_lookup_insert(start_date, end_date, target_intersections);
    
    --clear table before inserting
    DELETE FROM miovision_api.unacceptable_gaps
    WHERE
        dt >= start_date::date
        AND dt < end_date::date
        AND intersection_uid = ANY(target_intersections);
    
    --find intersections active each day
    WITH daily_intersections AS (
        SELECT DISTINCT
            datetime_bin::date AS dt,
            v.intersection_uid
        FROM miovision_api.volumes AS v
        INNER JOIN miovision_api.intersections AS i USING (intersection_uid)
        WHERE
            v.datetime_bin >= start_date
            AND v.datetime_bin < end_date
            AND v.datetime_bin >= i.date_installed
            AND (
                v.datetime_bin < i.date_decommissioned
                OR i.date_decommissioned IS NULL
            )
            AND i.intersection_uid = ANY(target_intersections)
    ),

    --combine the artificial and actual datetime_bins. 
    fluffed_data AS (
        --add the start and end of the day interval for each active intersection
        --to make sure the gaps are not open ended. 
        SELECT
            i.dt,
            i.intersection_uid,
            bins.datetime_bin,
            interval '0 minutes' AS gap_adjustment --don't need to reduce gap width for artificial data
        FROM daily_intersections AS i
        --add artificial data points at start and end of each day to find gaps overlapping start/end.
        CROSS JOIN LATERAL (
            VALUES
                --catch gaps overlapping days
                (i.dt - interval '15 minutes'),
                (i.dt + interval '1 day')
        ) AS bins(datetime_bin)

        --group by in next step takes care of duplicates
        UNION ALL

        --the distinct datetime bins which did appear in the day's data. 
        SELECT DISTINCT
            datetime_bin::date,
            intersection_uid,
            datetime_bin,
            --need to reduce gap length by 1 minute for real data since that minute contains data
            interval '1 minute' AS gap_adjustment
        FROM miovision_api.volumes
        WHERE
            datetime_bin >= start_date - interval '15 minutes'
            AND datetime_bin < end_date
            AND intersection_uid = ANY(target_intersections)
    ),

    --looks at sequential bins to identify breaks larger than 1 minute.
    bin_times AS (
        SELECT
            dt,
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
        LEFT JOIN ref.holiday AS hol USING (dt)
        GROUP BY
            dt,
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
        --distinct on to remove duplicates in rare case where there
        --are two distinct gaps which overlap the same time bin. 
        SELECT DISTINCT ON (intersection_uid, datetime_bin)
            bt.dt,
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
                end_date,
                interval '15 minutes'
        ) AS bins(datetime_bin) ON
            bins.datetime_bin >= datetime_bin(bt.gap_start, 15)
            AND bins.datetime_bin < datetime_bin_ceil(bt.gap_end, 15)
        --find the acceptable gap size based on historical lookback.
        JOIN miovision_api.gapsize_lookup AS gl ON
            gl.intersection_uid = bt.intersection_uid
            AND gl.hour_bin = date_part('hour', bins.datetime_bin)
            AND gl.dt = bt.dt
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
            --gap entirely outside of days range
            AND NOT(bt.gap_end <= bt.dt)
        ORDER BY
            bt.intersection_uid,
            bins.datetime_bin,
            gm.gap_minutes_total DESC, --keep the larger gap
            bt.gap_start --and the one that started first
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

COMMENT ON FUNCTION miovision_api.find_gaps(timestamp, timestamp, integer [])
IS 'Function to identify gaps in miovision_api.volumes data and insert into
miovision_api.unacceptable_gaps table. gap_tolerance set using 60 day 
lookback avg volumes and thresholds defined in miovision_api.gapsize_lookup.
Contains optional intersection parameter.';

ALTER FUNCTION miovision_api.find_gaps(timestamp, timestamp, integer [])
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.find_gaps(timestamp, timestamp, integer [])
TO miovision_api_bot;