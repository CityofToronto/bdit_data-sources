CREATE OR REPLACE VIEW miovision_api.monitor_intersection_movements AS (
    WITH intersection_classification_totals AS (
        SELECT
            intersection_uid,
            classification_uid,
            SUM(daily_volume) AS volume
        FROM miovision_api.volumes_daily_unfiltered
        WHERE dt >= CURRENT_DATE - 100
        GROUP BY
            intersection_uid,
            classification_uid
    )

    SELECT
        v.intersection_uid,
        v.classification_uid,
        v.leg,
        v.movement_uid,
        m.movement_name,
        c.classification,
        SUM(v.volume) AS sum_volume,
        ict.volume AS intersection_classification_total,
        SUM(v.volume) / ict.volume::numeric AS volume_frac
    FROM miovision_api.volumes AS v
    --anti join intersection_movements_denylist
    LEFT JOIN miovision_api.intersection_movements_denylist AS im_dl
        USING (intersection_uid, classification_uid, leg, movement_uid)
    LEFT JOIN miovision_api.classifications AS c USING (classification_uid)
    LEFT JOIN miovision_api.movements AS m USING (movement_uid)
    LEFT JOIN intersection_classification_totals AS ict USING (intersection_uid, classification_uid)
    WHERE
        v.volume_15min_mvt_uid IS NULL --not aggregated
        AND v.datetime_bin >= CURRENT_DATE - 100
        AND NOT (v.classification_uid = 10 AND movement_uid = 8) --bike exit
        AND im_dl.intersection_uid IS NULL
    GROUP BY
        v.intersection_uid,
        v.classification_uid,
        v.leg,
        v.movement_uid,
        v.intersection_uid,
        ict.volume,
        m.movement_name,
        c.classification
    HAVING
        SUM(v.volume) > 1000 --10 per day avg
        AND SUM(v.volume) / ict.volume::numeric > 0.005
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
