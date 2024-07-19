--DROP VIEW ecocounter.eco_status_dashboard IF EXISTS;

CREATE OR REPLACE VIEW ecocounter.eco_status_dashboard AS
SELECT
    e.site_id::text AS unique_id,
    'Eco-Counter'::text AS device_family,
    'Inductive'::text AS det_tech,
    CASE
        WHEN last_active >= now()::date - interval '2 day' THEN 'Online'::text
        WHEN last_active >= now()::date - interval '14 day' THEN 'Not Reporting'::text
        WHEN date_decommissioned IS NOT NULL THEN 'Decommissioned'::text
        --anomalous_range = malfunctioning
        WHEN last_active < now()::date - interval '14 day' THEN 'Offline'::text
        ELSE 'Unknown'::text
    END AS status,
    e.site_description AS location_name,
    e.geom,
    e.first_active::date AS date_installed,
    e.last_active::date AS last_active,
    e.centreline_id
FROM ecocounter.sites e
ORDER BY e.site_id;

ALTER TABLE ecocounter.eco_status_dashboard OWNER TO ckousin;
GRANT SELECT ON TABLE ecocounter.eco_status_dashboard TO bdit_humans;
GRANT ALL ON TABLE ecocounter.eco_status_dashboard TO ckousin;

