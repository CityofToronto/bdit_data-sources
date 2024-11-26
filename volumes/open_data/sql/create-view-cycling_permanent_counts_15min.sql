-- View: open_data.cycling_permanent_counts_15min

-- DROP TABLE IF EXISTS open_data.cycling_permanent_counts_15min;

CREATE VIEW open_data.cycling_permanent_counts_15min AS
SELECT
    site_description::text AS location_name,
    direction::text AS direction,
    datetime_bin,
    bin_volume::integer
FROM ecocounter.open_data_15min_counts;

ALTER TABLE IF EXISTS open_data.cycling_permanent_counts_15min OWNER TO od_admins;

REVOKE ALL ON TABLE open_data.cycling_permanent_counts_15min FROM bdit_humans;
GRANT SELECT ON TABLE open_data.cycling_permanent_counts_15min TO bdit_humans;

GRANT SELECT ON TABLE open_data.cycling_permanent_counts_15min TO ecocounter_bot;

COMMENT ON VIEW open_data.cycling_permanent_counts_15min IS
'15 minute cycling volumes from various (currently just Eco-Counter) permanent counting stations.';