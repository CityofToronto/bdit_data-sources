CREATE VIEW miovision_api.monitor_intersection_movements AS (
    WITH intersection_movements AS (
        SELECT
            v.intersection_uid,
            v.classification_uid,
            v.leg,
            v.movement_uid,
            im.intersection_uid IS NULL AS is_not_in_intersection_movements,
            SUM(v.volume) AS sum_volume,
            SUM(SUM(v.volume)) OVER (PARTITION BY v.intersection_uid, v.classification_uid) AS intersection_classification_total
        FROM miovision_api.volumes AS v
        LEFT JOIN miovision_api.intersection_movements AS im
            USING (intersection_uid, classification_uid, leg, movement_uid)
        WHERE
            v.datetime_bin >= now()::date - interval '100 days'
            AND v.classification_uid IN (1, 2, 6, 10)
            AND NOT(v.classification_uid = 10 AND movement_uid = 8) --bike exit
        GROUP BY
            v.intersection_uid,
            v.classification_uid,
            v.leg,
            v.movement_uid,
            im.intersection_uid
        HAVING SUM(v.volume) > 1000 --10 per day avg
    )

    SELECT
        im.intersection_uid,
        im.classification_uid,
        im.leg,
        im.movement_uid,
        i.intersection_name,
        c.classification,
        im.sum_volume,
        im.sum_volume / im.intersection_classification_total AS volume_frac
    FROM intersection_movements AS im
    JOIN miovision_api.intersections AS i USING (intersection_uid)
    JOIN miovision_api.classifications AS c USING (classification_uid)
    WHERE
        im.is_not_in_intersection_movements
        AND im.sum_volume / im.intersection_classification_total > 0.005
    ORDER BY volume_frac DESC
);

ALTER VIEW miovision_api.monitor_intersection_movements OWNER TO miovision_admins;

GRANT SELECT ON TABLE miovision_api.monitor_intersection_movements TO bdit_humans;
GRANT SELECT ON TABLE miovision_api.monitor_intersection_movements TO miovision_api_bot;

COMMENT ON VIEW miovision_api.monitor_intersection_movements
IS 'View to monitor intersection_movements table for missing but common movements.
These could result from changes to turning movement restrictions, road geometry, 
change in Miovision configuration etc. This view is monitored by a weekly
Airflow process.';