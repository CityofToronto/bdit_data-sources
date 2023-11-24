--DROP MATERIALIZED VIEW gwolofs.daily_vehicle_volumes;
--CREATE MATERIALIZED VIEW gwolofs.daily_vehicle_volumes AS 

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

SELECT
    v.intersection_uid,
    v.datetime_bin::date AS dt,
    SUM(v.volume) AS vehicle_volume,
    date_part('isodow', v.datetime_bin::date) AS isodow,
    CASE 
        WHEN holiday.holiday IS NOT NULL THEN 1
        ELSE 0
    END AS holiday,
    COALESCE(un.unacceptable_gap_minutes, 0) AS unacceptable_gap_minutes,
    60 * 24 - COUNT(DISTINCT datetime_bin) AS datetime_bins_missing
--raw data
FROM miovision_api.volumes AS v
INNER JOIN miovision_api.classifications AS c USING (classification_uid)
LEFT JOIN ref.holiday ON holiday.dt = v.datetime_bin::date
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
        ar.classification_uid = v.classification_uid
        OR ar.classification_uid IS NULL
    )
    AND v.datetime_bin >= LOWER(ar.time_range)
    AND v.datetime_bin < UPPER(ar.time_range)
    AND ar.problem_level IN ('do-not-use', 'questionable')
WHERE
    ar.time_range IS NULL --anti join anomalous ranges
    AND v.datetime_bin >= '2023-11-23'::date
    AND c.class_type = 'Vehicles' --all types
GROUP BY
    v.intersection_uid,
    v.datetime_bin::date,
    holiday.holiday,
    un.unacceptable_gap_minutes
HAVING SUM(v.volume) > 0
ORDER BY
    v.datetime_bin::date,
    v.intersection_uid;
    
--SELECT * FROM gwolofs.daily_vehicle_volumes WHERE dt >= '2023-11-20'::date

COMMENT ON TABLE gwolofs.daily_vehicle_volumes IS 'Daily vehicle volumes. Excludes `anomalous_ranges` (use discouraged based on investigations) but does not exclude time around `unacceptable_gaps` (zero volume periods).'
COMMENT ON COLUMN gwolofs.daily_vehicle_volumes.isodow IS 'Use `WHERE iso dow <= 5 AND holiday = 0` for non-holiday weekdays.'
COMMENT ON COLUMN gwolofs.daily_vehicle_volumes.unacceptable_gap_minutes IS 'Periods of consecutive zero volumes deemed unacceptable based on avg intersection volume in that hour.'
COMMENT ON COLUMN gwolofs.daily_vehicle_volumes.datetime_bins_missing IS 'Minutes with zero vehicle volumes out of a total of possible 1440 minutes.'
