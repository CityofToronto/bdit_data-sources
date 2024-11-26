-- View: open_data.cycling_permanent_counts_locations
-- DROP VIEW open_data.cycling_permanent_counts_locations;

CREATE OR REPLACE VIEW open_data.cycling_permanent_counts_locations AS

SELECT 
    location_name,
    direction,
    linear_name_full,
    side_street,
    lng,
    lat,
    centreline_id,
    bin_size,
    latest_calibration_study,
    first_active,
    last_active,
    date_decommissioned,
    technology
FROM ecocounter.open_data_locations
ORDER BY
    location_name,
    direction;

ALTER TABLE open_data.cycling_permanent_counts_locations OWNER TO od_admins;

GRANT SELECT ON TABLE open_data.cycling_permanent_counts_locations TO bdit_humans;
GRANT SELECT ON TABLE open_data.cycling_permanent_counts_locations TO ecocounter_bot;
