-- View: ecocounter.open_data_locations
-- DROP VIEW ecocounter.open_data_locations;

CREATE OR REPLACE VIEW ecocounter.open_data_locations AS
WITH od_flows AS (
    SELECT
        site_id,
        direction AS direction_main,
        MIN(dt::date) AS min_date,
        MAX(dt::date) AS max_date
    FROM ecocounter.open_data_daily_counts
    GROUP BY
        site_id,
        direction
)

SELECT DISTINCT ON (s.site_description, f.direction_main::text)
    s.site_description AS location_name,
    f.direction_main::text AS direction,
    s.linear_name_full,
    s.side_street,
    ROUND(st_x(s.geom)::numeric, 7) AS lng,
    ROUND(st_y(s.geom)::numeric, 7) AS lat,
    s.centreline_id,
    f.bin_size,
    od_flows.min_date AS first_active,
    od_flows.max_date AS last_active,
    s.date_decommissioned::date AS date_decommissioned,
    cf.count_date AS latest_calibration_study,
    s.technology
FROM ecocounter.flows AS f
--omit sites if they have no data to publish.
JOIN od_flows USING (site_id, direction_main)
JOIN ecocounter.sites AS s USING (site_id)
LEFT JOIN ecocounter.calibration_factors AS cf USING (flow_id)
ORDER BY
    s.site_description ASC,
    f.direction_main::text ASC,
    cf.count_date DESC NULLS LAST;

ALTER TABLE ecocounter.open_data_locations OWNER TO ecocounter_admins;

GRANT SELECT ON TABLE ecocounter.open_data_locations TO bdit_humans;
GRANT ALL ON TABLE ecocounter.open_data_locations TO ecocounter_admins;
GRANT SELECT ON TABLE ecocounter.open_data_locations TO ecocounter_bot;
