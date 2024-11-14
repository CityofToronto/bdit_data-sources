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
    s.facility_description,
    s.replaced_by_site_id,
    s.centreline_id,
    s.first_active,
    s.date_decommissioned,
    string_agg(DISTINCT f.direction_main::text, ', '::text) AS site_directions,
    --could also turn this into an array?
    MAX(cf.count_date) AS latest_calibration_study,
    MIN(f.bin_size) AS bin_size --Note: each site has a consistent bin_size, although it is defined in the API at the flow level.
FROM ecocounter.sites s
JOIN od_sites USING (site_id) --omit sites if they have no data to publish.
JOIN ecocounter.flows f USING (site_id)
LEFT JOIN ecocounter.calibration_factors AS cf USING (flow_id)
GROUP BY
    s.site_id,
    od_sites.site_id,
    s.site_description,
    s.geom,
    s.facility_description,
    s.notes,
    s.replaced_by_site_id,
    s.centreline_id,
    s.first_active,
    s.date_decommissioned
ORDER BY site_description;

ALTER TABLE ecocounter.open_data_sites
    OWNER TO ecocounter_admins;

GRANT SELECT ON TABLE ecocounter.open_data_sites TO bdit_humans;
GRANT ALL ON TABLE ecocounter.open_data_sites TO ecocounter_admins;
