CREATE OR REPLACE FUNCTION miovision_api.gapsize_lookup_insert(
    start_date timestamp,
    end_date timestamp,
    intersections integer []
)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE
AS $BODY$
    BEGIN

    DELETE FROM miovision_api.gapsize_lookup
    WHERE
        dt >= start_date
        AND dt < end_date
        AND intersection_uid = ANY(gapsize_lookup_insert.intersections);

    WITH study_dates AS (
        SELECT
            dt,
            NOT(date_part('isodow', dates.dt) <= 5 AND hol.holiday IS NULL) AS weekend
        FROM generate_series(
            start_date::date,
            end_date::date - interval '1 day', --don't include end_date
            '1 day'::interval
        ) dates(dt)
        LEFT JOIN ref.holiday AS hol USING (dt)
    ), 
    
    hourly_volumes AS (
        SELECT
            dates.dt,
            v.intersection_uid,
            v.classification_uid,
            NOT(date_part('isodow', dates.dt) <= 5 AND hol.holiday IS NULL) AS weekend,
            date_part('hour', v.datetime_bin)::smallint AS hour_bin,
            SUM(v.volume) AS vol
        FROM miovision_api.volumes AS v,
        LATERAL (
            SELECT datetime_bin::date AS dt
        ) dates
        LEFT JOIN ref.holiday AS hol USING (dt)
        WHERE
            v.datetime_bin >= start_date - interval '60 days'
            AND v.datetime_bin < end_date
            AND v.intersection_uid = ANY(gapsize_lookup_insert.intersections)
        GROUP BY
            dates.dt,
            v.intersection_uid,
            --group both by individual classificaiton and all classifications.
            GROUPING SETS ((v.classification_uid), ()),
            hour_bin,
            hol.holiday
        
        UNION 
        
        --padding values in case the intersection didn't appear on previous day, but
        --did appear within the 60 day lookback, inorder to catch these with window function
        SELECT
            dt,
            intersection_uid,
            classification_uid,
            weekend,
            hour_bin,
            NULL AS volume
        FROM study_dates
        CROSS JOIN miovision_api.intersections
        CROSS JOIN (
            SELECT classification_uid FROM miovision_api.classifications
            UNION SELECT NULL::integer --represents all classifications
        ) AS classifications
        CROSS JOIN generate_series(0, 23, 1) AS hours(hour_bin)
        WHERE intersection_uid = ANY(gapsize_lookup_insert.intersections)
    ),
    
    lookback_avgs AS (
        SELECT DISTINCT ON ( --get rid of duplicates from padding
            dt, intersection_uid, classification_uid, weekend, hour_bin
        )
            dt,
            intersection_uid,
            classification_uid,
            weekend,
            hour_bin,
            AVG(vol) OVER w AS avg_hour_vol
        FROM hourly_volumes
        WINDOW w AS (
            --60 day lookback for same intersection, classification, day type, hour
            PARTITION BY intersection_uid, classification_uid, weekend, hour_bin
            ORDER BY dt RANGE BETWEEN
                interval '60 days' PRECEDING AND interval '1 days' PRECEDING
        )
        ORDER BY 
            dt,
            intersection_uid,
            classification_uid,
            weekend,
            hour_bin,
            vol DESC NULLS LAST
    )
    
        --avg of hourly volume by intersection, hour of day, day type to determine gap_tolerance
        INSERT INTO miovision_api.gapsize_lookup(
            dt, intersection_uid,
            classification_uid,
            hour_bin, weekend, avg_hour_vol, gap_tolerance
        )
        SELECT
            study_dates.dt,
            lba.intersection_uid,
            lba.classification_uid,
            lba.hour_bin,
            study_dates.weekend,
            lba.avg_hour_vol,
            --only designate an acceptable gap size for total volume, not for individual classifications.
            CASE WHEN lba.classification_uid IS NULL THEN
                CASE
                    WHEN lba.avg_hour_vol >= 1500::numeric THEN 5::smallint
                    WHEN lba.avg_hour_vol >= 500::numeric THEN 10::smallint
                    WHEN lba.avg_hour_vol >= 100::numeric THEN 15::smallint
                    WHEN lba.avg_hour_vol < 100::numeric THEN 20::smallint
                    ELSE NULL::smallint
                END
            END AS gap_tolerance
        FROM study_dates
        LEFT JOIN lookback_avgs AS lba USING (weekend, dt)
        WHERE avg_hour_vol IS NOT NULL; --nothing in lookback
END;
$BODY$;

ALTER FUNCTION miovision_api.gapsize_lookup_insert(timestamp, timestamp, integer [])
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.gapsize_lookup_insert(timestamp, timestamp, integer [])
TO miovision_api_bot;

COMMENT ON FUNCTION miovision_api.gapsize_lookup_insert(timestamp, timestamp, integer [])
IS 'Determine the average volumes for each hour/intersection/daytype/classification
based on a 60 day lookback. Uses GROUPING SETS to identify both volume for individual
classifications and total interseciton volumes (classification_uid IS NULL).
Total intersection volume is used to determine a minimum gap duration
for use in unacceptable_gaps.';