-- View: ecocounter.open_data_sites
-- DROP VIEW ecocounter.open_data_sites;

CREATE OR REPLACE VIEW ecocounter.open_data_sites AS
WITH od_sites AS (
    SELECT DISTINCT site_id
    FROM ecocounter.open_data_daily_counts
)

SELECT
    s.site_id,
    s.site_description,
    st_x(s.geom) AS site_x,
    st_y(s.geom) AS site_y,
    s.replaced_by_site_id,
    s.first_active::date AS first_active,
    s.date_decommissioned::date AS date_decommissioned,
    CASE
        WHEN site_id = 210 THEN 'Induction - Other'
        ELSE 'Induction - Eco-Counter'
    END AS technology
FROM ecocounter.sites AS s
JOIN od_sites USING (site_id) --omit sites if they have no data to publish.
JOIN ecocounter.flows USING (site_id)
LEFT JOIN ecocounter.calibration_factors USING (flow_id)
ORDER BY s.site_description;

ALTER TABLE ecocounter.open_data_sites OWNER TO ecocounter_admins;

GRANT SELECT ON TABLE ecocounter.open_data_sites TO bdit_humans;
GRANT ALL ON TABLE ecocounter.open_data_sites TO ecocounter_admins;
