CREATE VIEW miovision_api.monitor_intersection_movements AS (
    WITH intersection_movements AS (
        SELECT
            intersection_uid,
            classification_uid,
            leg,
            movement_uid,
            im.intersection_uid IS NULL AS is_not_in_intersection_movements,
            SUM(volume) AS sum_volume,
            SUM(SUM(volume)) OVER (PARTITION BY intersection_uid, classification_uid) AS intersection_classification_total
        FROM miovision_api.volumes
        LEFT JOIN miovision_api.intersection_movements AS im USING (intersection_uid, classification_uid, leg, movement_uid)
        WHERE
            datetime_bin >= now()::date - interval '100 days'
            AND classification_uid IN (1,2,6,10)
            AND NOT(classification_uid = 10 AND movement_uid = 8) --bike exit
        GROUP BY intersection_uid, classification_uid, leg, movement_uid, im.intersection_uid
        HAVING SUM(volume) > 1000 --10 per day avg
    )

    SELECT
        intersection_uid,
        classification_uid,
        leg,
        movement_uid,
        intersection_name,
        classification,
        sum_volume,
        sum_volume / intersection_classification_total AS volume_frac
    FROM intersection_movements
    JOIN miovision_api.intersections USING (intersection_uid)
    JOIN miovision_api.classifications USING (classification_uid)
    WHERE
        is_not_in_intersection_movements
        AND sum_volume / intersection_classification_total > 0.005
    ORDER BY volume_frac DESC
);

ALTER VIEW miovision_api.monitor_intersection_movements OWNER TO miovision_admins;

GRANT SELECT ON VIEW miovision_api.monitor_intersection_movements TO bdit_humans;
GRANT SELECT ON VIEW miovision_api.monitor_intersection_movements TO miovision_api_bot;

COMMENT ON VIEW miovision_api.monitor_intersection_movements
IS 'View to monitor intersection_movements table for missing but common movements.
These could result from changes to turning movement restrictions, road geometry, 
change in Miovision configuration etc. This view is monitored by a weekly
Airflow process.'