CREATE OR REPLACE FUNCTION miovision_api.gapsize_lookup_insert(
    start_date timestamp
)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE
AS $BODY$
    DECLARE is_weekend boolean :=
        NOT(date_part('isodow', start_date) <= 5
            AND (SELECT holiday FROM ref.holiday WHERE dt = start_date) IS NULL);

    BEGIN

    DELETE FROM miovision_api.gapsize_lookup
    WHERE dt = start_date;
    
    WITH hourly_volumes AS (
        SELECT
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
            AND v.datetime_bin < start_date
        GROUP BY
            v.intersection_uid,
            --group both by individual classificaiton and all classificaitons.
            GROUPING SETS ((v.classification_uid), ()),
            weekend,
            hour_bin,
            dates.dt
    )
    
        --avg of hourly volume by intersection, hour of day, day type to determine gap_tolerance
        INSERT INTO miovision_api.gapsize_lookup(
            dt, intersection_uid,
            classification_uid,
            hour_bin, weekend, avg_hour_vol, gap_tolerance
        )
        SELECT
            start_date,
            intersection_uid,
            classification_uid,
            hour_bin,
            weekend,
            AVG(vol) AS avg_hour_vol,
            --only designate an acceptable gap size for total volume, not for individual classifications.
            CASE WHEN classification_uid IS NULL THEN
                CASE
                    WHEN AVG(vol) < 100::numeric THEN 20::smallint
                    WHEN AVG(vol) >= 100::numeric AND AVG(vol) < 500::numeric THEN 15::smallint
                    WHEN AVG(vol) >= 500::numeric AND AVG(vol) < 1500::numeric THEN 10::smallint
                    WHEN AVG(vol) > 1500::numeric THEN 5::smallint
                    ELSE NULL::smallint
                END
            END AS gap_tolerance
        FROM hourly_volumes
        WHERE weekend = is_weekend
        GROUP BY
            intersection_uid,
            classification_uid,
            weekend,
            hour_bin;

END;
$BODY$;

ALTER FUNCTION miovision_api.gapsize_lookup_insert
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.gapsize_lookup_insert
TO miovision_api_bot;

COMMENT ON FUNCTION miovision_api.gapsize_lookup_insert
IS 'Determine the average volumes for each hour/intersection/daytype/classification
based on a 60 day lookback. Uses GROUPING SETS to identify both volume for individual
classifications and total interseciton volumes (classification_uid IS NULL).
Total intersection volume is used to determine a minimum gap duration
for use in unacceptable_gaps.';