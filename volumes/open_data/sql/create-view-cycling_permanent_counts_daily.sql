-- View: open_data.cycling_permanent_counts_daily

-- DROP TABLE IF EXISTS open_data.cycling_permanent_counts_daily;

CREATE VIEW open_data.cycling_permanent_counts_daily AS
SELECT
    site_description::text AS location_name,
    direction::text AS direction,
    dt::date,
    daily_volume::integer
FROM ecocounter.open_data_daily_counts;

ALTER TABLE IF EXISTS open_data.cycling_permanent_counts_daily OWNER TO od_admins;

REVOKE ALL ON TABLE open_data.cycling_permanent_counts_daily FROM bdit_humans;
GRANT SELECT ON TABLE open_data.cycling_permanent_counts_daily TO bdit_humans;

GRANT SELECT ON TABLE open_data.cycling_permanent_counts_daily TO ecocounter_bot;

COMMENT ON VIEW open_data.cycling_permanent_counts_daily IS
'Daily cycling volumes from various (currently just Eco-Counter) permanent counting stations.';