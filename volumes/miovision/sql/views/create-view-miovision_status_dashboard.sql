CREATE OR REPLACE VIEW miovision_api.mio_dashboard_data AS

WITH open_alerts AS (
    SELECT
        intersection_uid,
        string_agg(DISTINCT alert, ', ') AS alerts
    FROM miovision_api.alerts
    WHERE end_time IS NULL
    GROUP BY intersection_uid
),

open_anomalous_ranges AS (
    SELECT
        intersection_uid,
        string_agg(uid::text, ', ') AS uids
    FROM miovision_api.anomalous_ranges
    WHERE
        problem_level <> 'valid-caveat'::text
        AND (
            range_end IS NULL
            OR (
                notes ~~ '%identified by a daily airflow process%'::text
                AND range_end = (now() AT TIME ZONE 'Canada/Eastern'::text)::date
            )
        )
    GROUP BY intersection_uid
),

last_active AS (
    SELECT
        intersection_uid,
        MAX(dt) AS max_dt 
    FROM miovision_api.volumes_daily_unfiltered
    GROUP BY intersection_uid
)

SELECT
    i.id AS unique_id,
    'Miovision'::text AS device_family,
    CASE
        WHEN last_active.max_dt < (now() AT TIME ZONE 'Canada/Eastern'::text - interval '14 days')::date THEN 'Offline'
        WHEN open_alerts.alerts IS NOT NULL OR open_anomalous_ranges.uids IS NOT NULL THEN 'Malfunctioning'
        WHEN last_active.max_dt = (now() AT TIME ZONE 'Canada/Eastern'::text - interval '1 day')::date THEN 'Online'
        WHEN i.date_decommissioned IS NOT NULL THEN 'Decommissioned'
    END AS status,
    i.api_name AS location_name,
    i.geom,
    'Video' AS det_tech,
    i.date_installed,
    COALESCE(last_active.max_dt, date_decommissioned) AS last_active,
    i.px,
    i.int_id
FROM miovision_api.intersections AS i
LEFT JOIN open_anomalous_ranges USING (intersection_uid)
LEFT JOIN open_alerts USING (intersection_uid)
LEFT JOIN last_active USING (intersection_uid);

ALTER TABLE miovision_api.mio_dashboard_data OWNER TO ckousin;
GRANT ALL ON TABLE miovision_api.mio_dashboard_data TO ckousin;

GRANT SELECT ON TABLE miovision_api.mio_dashboard_data TO bdit_humans;