-- View: ecocounter.open_data_flows
-- DROP VIEW ecocounter.open_data_flows;

CREATE OR REPLACE VIEW ecocounter.open_data_flows AS
WITH od_flows AS (
    SELECT DISTINCT
        site_id,
        direction AS direction_main
    FROM ecocounter.open_data_daily_counts
)

SELECT DISTINCT ON (f.site_id, f.direction_main::text)
    f.site_id,
    --centreline is defined at the site level here, but would be at the flow level for Miovision
    s.centreline_id,
    f.direction_main::text,
    f.bin_size,
    cf.count_date AS latest_calibration_study
FROM ecocounter.flows AS f
JOIN od_flows USING (site_id, direction_main) --omit sites if they have no data to publish.
JOIN ecocounter.sites AS s USING (site_id)
LEFT JOIN ecocounter.calibration_factors AS cf USING (flow_id)
ORDER BY f.site_id ASC, f.direction_main::text, cf.count_date DESC;

ALTER TABLE ecocounter.open_data_flows OWNER TO ecocounter_admins;

GRANT SELECT ON TABLE ecocounter.open_data_flows TO bdit_humans;
GRANT ALL ON TABLE ecocounter.open_data_flows TO ecocounter_admins;
