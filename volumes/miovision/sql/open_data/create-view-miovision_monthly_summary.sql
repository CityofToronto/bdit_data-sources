--need to think more about grouping modes.

DROP TABLE gwolofs.miovision_open_data_monthly;
CREATE TABLE gwolofs.miovision_open_data_monthly AS 

--replace with query of miovision_api.volumes_daily?
WITH daily_volumes AS (
    SELECT
        v.intersection_uid,
        v.datetime_bin::date AS dt,
        c.class_type,
        --v.leg,
        SUM(v.volume) AS total_vol,
        COUNT(DISTINCT v.datetime_bin)
    FROM miovision_api.volumes_15min_mvt AS v
    JOIN miovision_api.classifications AS c USING (classification_uid)
    WHERE
        v.datetime_bin >= '2024-02-01'::date
        AND v.datetime_bin < '2024-03-01'::date
        AND date_part('isodow', v.datetime_bin) <= 5 --weekday
        AND c.class_type IS NOT NULL
    GROUP BY
        v.intersection_uid,
        c.class_type,
        dt--,
        --v.leg
    --these counts are very low for trucks, etc, doesn't work as a filter
    --HAVING COUNT(DISTINCT datetime_bin) >= 92 --4 15minute bins present
    ORDER BY
        v.intersection_uid,
        c.class_type,
        --v.leg,
        dt
),

v15 AS (
    --group multiple classifications together by classtype
    SELECT
        v.intersection_uid,
        v.datetime_bin,
        c.class_type, --need to refine this grouping
        --v.leg,
        SUM(v.volume) AS vol_15min
    FROM miovision_api.volumes_15min_mvt AS v
    JOIN miovision_api.classifications AS c USING (classification_uid)
    WHERE
        datetime_bin >= '2024-02-01'::date
        AND datetime_bin < '2024-03-01'::date
        AND date_part('isodow', datetime_bin) IN (1,2,3,4,5) --weekday
        AND class_type IS NOT NULL
        --AND classification_uid NOT IN (2,10) --exclude bikes due to reliability
    GROUP BY
        v.intersection_uid,
        c.class_type,
        v.datetime_bin--,
        --v.leg
    --HAVING COUNT(DISTINCT datetime_bin) = 4 --can't use this filter for the non-auto modes
),

hourly_data AS (
    --find rolling 1 hour volume
    SELECT
        intersection_uid,
        --leg,
        class_type,
        datetime_bin,
        datetime_bin::date AS dt,
        CASE WHEN date_part('hour', datetime_bin) < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
        vol_15min,
        SUM(vol_15min) OVER (
            PARTITION BY intersection_uid, class_type --, leg
            ORDER BY datetime_bin
            RANGE BETWEEN '45 minutes' PRECEDING AND CURRENT ROW
        ) AS hr_vol
    FROM v15
),

highest_daily_volume AS (
    --find highest volume each day
    SELECT
        intersection_uid,
        --leg,
        class_type,
        am_pm,
        dt,
        MAX(hr_vol) AS max_hr_volume
    FROM hourly_data
    GROUP BY
        intersection_uid,
        --leg,
        class_type,
        am_pm,
        dt
),

anomalous_ranges_classtype AS (
    SELECT
        ar.uid,
        c.class_type,
        ar.classification_uid,
        ar.intersection_uid,
        ar.range_start,
        ar.range_end,
        ar.notes,
        ar.problem_level
    FROM miovision_api.anomalous_ranges AS ar
    LEFT JOIN miovision_api.classifications AS c USING (classification_uid)
)

SELECT
    coalesce(dv.intersection_uid, hv.intersection_uid) AS intersection,
    --coalesce(dv.leg, hv.leg) AS leg,
    date_trunc('month', coalesce(dv.dt, hv.dt)) AS mnth,
    ROUND(AVG(dv.total_vol) FILTER (WHERE dv.class_type = 'Vehicles'), 0) AS avg_daily_vol_veh,
    ROUND(AVG(dv.total_vol) FILTER (WHERE dv.class_type = 'Pedestrians'), 0) AS avg_daily_vol_ped,
    ROUND(AVG(dv.total_vol) FILTER (WHERE dv.class_type = 'Cyclists'), 0) AS avg_daily_vol_bike,
    COUNT(dv.*) FILTER (WHERE dv.class_type = 'Vehicles') AS veh_n_days,
    COUNT(dv.*) FILTER (WHERE dv.class_type = 'Pedestrians') AS ped_n_days,
    COUNT(dv.*) FILTER (WHERE dv.class_type = 'Cyclists') AS bike_n_days,
    ROUND(AVG(hv.max_hr_volume) FILTER (WHERE hv.class_type = 'Vehicles' AND hv.am_pm = 'AM'), 0) AS avg_am_peak_hour_vol_veh,
    ROUND(AVG(hv.max_hr_volume) FILTER (WHERE hv.class_type = 'Pedestrians' AND hv.am_pm = 'AM'), 0) AS avg_am_peak_hour_vol_ped,
    ROUND(AVG(hv.max_hr_volume) FILTER (WHERE hv.class_type = 'Cyclists' AND hv.am_pm = 'AM'), 0) AS avg_am_peak_hour_vol_bike,
    COUNT(hv.*) FILTER (WHERE hv.class_type = 'Vehicles' AND hv.am_pm = 'AM') AS veh_n_am_hrs,
    COUNT(hv.*) FILTER (WHERE hv.class_type = 'Pedestrians' AND hv.am_pm = 'AM') AS ped_n_am_hrs,
    COUNT(hv.*) FILTER (WHERE hv.class_type = 'Cyclists' AND hv.am_pm = 'AM') AS bike_n_am_hrs,
    ROUND(AVG(hv.max_hr_volume) FILTER (WHERE hv.class_type = 'Vehicles' AND hv.am_pm = 'PM'), 0) AS avg_pm_peak_hour_vol_veh,
    ROUND(AVG(hv.max_hr_volume) FILTER (WHERE hv.class_type = 'Pedestrians' AND hv.am_pm = 'PM'), 0) AS avg_pm_peak_hour_vol_ped,
    ROUND(AVG(hv.max_hr_volume) FILTER (WHERE hv.class_type = 'Cyclists' AND hv.am_pm = 'PM'), 0) AS avg_pm_peak_hour_vol_bike,
    COUNT(hv.*) FILTER (WHERE hv.class_type = 'Vehicles' AND hv.am_pm = 'PM') AS veh_n_pm_hrs,
    COUNT(hv.*) FILTER (WHERE hv.class_type = 'Pedestrians' AND hv.am_pm = 'PM') AS ped_n_pm_hrs,
    COUNT(hv.*) FILTER (WHERE hv.class_type = 'Cyclists' AND hv.am_pm = 'PM') AS bike_n_pm_hrs,
    array_agg(DISTINCT ar.notes) FILTER (WHERE ar.uid IS NOT NULL) AS notes
FROM daily_volumes AS dv
FULL JOIN highest_daily_volume AS hv USING (intersection_uid, dt, class_type--, leg
                                           )
LEFT JOIN ref.holiday AS hol
    ON hol.dt = coalesce(dv.dt, hv.dt)
LEFT JOIN anomalous_ranges_classtype ar ON
    (
        ar.intersection_uid = coalesce(dv.intersection_uid, hv.intersection_uid)
        OR ar.intersection_uid IS NULL
    ) AND (
        ar.class_type = coalesce(dv.class_type, hv.class_type)
        OR ar.class_type IS NULL
    )
    AND coalesce(dv.dt, hv.dt) >= ar.range_start
    AND (
        coalesce(dv.dt, hv.dt) < ar.range_end
        OR ar.range_end IS NULL
    )
WHERE hol.holiday IS NULL
GROUP BY
    coalesce(dv.intersection_uid, hv.intersection_uid),
    --coalesce(dv.leg, hv.leg),
    date_trunc('month', coalesce(dv.dt, hv.dt))
--need to refine anomalous_range exclusions, move earlier
--HAVING NOT array_agg(ar.problem_level) && ARRAY['do-not-use'::text, 'questionable'::text] -- 344 vs 163! --need to exclude at a previous stage
ORDER BY
    coalesce(dv.intersection_uid, hv.intersection_uid),
    --coalesce(dv.leg, hv.leg),
    date_trunc('month', coalesce(dv.dt, hv.dt));

SELECT * FROM gwolofs.miovision_open_data_monthly;