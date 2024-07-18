-- View: ckousin.eco_dashboard_data

DROP VIEW ckousin.eco_dashboard_data;

CREATE OR REPLACE VIEW ckousin.eco_dashboard_data
 AS
 SELECT e.site_id::text AS unique_id,
    'Eco-Counter'::text AS device_family,
    'Inductive'::text AS det_tech,
        CASE
            WHEN last_active >= now()::date - interval '2 day' THEN 'Online'::text
            WHEN last_active >= now()::date - interval '14 day' THEN 'Not Reporting'::text
            --add decommissioned to sites
            --anomalous_range = malfunctioning
            WHEN last_active < now()::date - interval '14 day' THEN 'Offline'::text
            ELSE 'Unknown'::text
        END AS status,
    e.site_description AS location_name,
    e.geom,
    e.first_active::date AS date_installed,
        CASE --create date_decommissioned in sites
            WHEN e.replaced_by_site_id IS NOT NULL THEN e.last_active::date
            ELSE NULL::date
        END AS date_decommissioned,
    e.centreline_id
   FROM ecocounter.sites e
  ORDER BY e.site_id;

ALTER TABLE ckousin.eco_dashboard_data OWNER TO ckousin;

GRANT SELECT ON TABLE ckousin.eco_dashboard_data TO bdit_humans;
GRANT ALL ON TABLE ckousin.eco_dashboard_data TO ckousin;

