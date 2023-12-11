CREATE OR REPLACE FUNCTION miovision_api.agg_daily_vehicle_volumes(
    start_date date,
    end_date date
)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE 
AS $BODY$
BEGIN

DELETE FROM miovision_api.daily_vehicle_volumes
WHERE 
    dt >= start_date
    AND dt < end_date;

WITH unacceptable_gaps_summarized AS (
    SELECT
        intersection_uid,
        gap_start::date AS gap_date,
        SUM(gap_minute) AS unacceptable_gap_minutes
    FROM miovision_api.unacceptable_gaps
    GROUP BY
        intersection_uid,
        gap_start::date
)

INSERT INTO miovision_api.daily_vehicle_volumes(
    intersection_uid, dt, vehicle_volume, isodow, holiday, unacceptable_gap_minutes, datetime_bins_missing
)
SELECT
    v.intersection_uid,
    v.datetime_bin::date AS dt,
    SUM(v.volume) AS vehicle_volume,
    date_part('isodow', v.datetime_bin::date) AS isodow,
    CASE 
        WHEN hol.holiday IS NOT NULL THEN True
        ELSE False
    END AS holiday,
    COALESCE(un.unacceptable_gap_minutes, 0) AS unacceptable_gap_minutes,
    60 * 24 - COUNT(DISTINCT datetime_bin) AS datetime_bins_missing
--raw data
FROM miovision_api.volumes AS v
INNER JOIN miovision_api.classifications AS c USING (classification_uid)
LEFT JOIN ref.holiday AS hol ON hol.dt = v.datetime_bin::date
--identify duration of unacceptable gaps (extended zero periods)
LEFT JOIN unacceptable_gaps_summarized AS un ON
    un.intersection_uid = v.intersection_uid
    AND v.datetime_bin::date = un.gap_date
--anti join anomalous_ranges table
LEFT JOIN miovision_api.anomalous_ranges AS ar ON
    (
        ar.intersection_uid = v.intersection_uid
        OR ar.intersection_uid IS NULL
    ) AND (
        --only anomalous ranges for vehicles
        ar.classification_uid = v.classification_uid
        OR ar.classification_uid IS NULL
    )
    AND v.datetime_bin >= LOWER(ar.time_range)
    AND v.datetime_bin < UPPER(ar.time_range)
    AND ar.problem_level IN ('do-not-use', 'questionable')
WHERE
    ar.time_range IS NULL --anti join anomalous ranges
    AND c.class_type = 'Vehicles' --all types
    AND v.datetime_bin >= start_date
    AND v.datetime_bin < end_date
GROUP BY
    v.intersection_uid,
    v.datetime_bin::date,
    hol.holiday,
    un.unacceptable_gap_minutes
HAVING SUM(v.volume) > 0
ORDER BY
    v.datetime_bin::date,
    v.intersection_uid;

END;
$BODY$;

COMMENT ON FUNCTION miovision_api.agg_daily_vehicle_volumes IS
'Function for inserting new daily vehicle volumes into miovision_api.daily_vehicle_volumes';

ALTER FUNCTION miovision_api.agg_daily_vehicle_volumes OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.agg_daily_vehicle_volumes TO miovision_api_bot;