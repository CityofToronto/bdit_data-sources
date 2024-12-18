-- View: open_data.cycling_permanent_counts_15min

-- DROP VIEW IF EXISTS open_data.cycling_permanent_counts_15min;

CREATE VIEW open_data.cycling_permanent_counts_15min AS
SELECT
    od.location_dir_id,
    eco.site_description::text AS location_name,
    eco.direction::text AS direction,
    eco.datetime_bin,
    eco.bin_volume::integer
FROM ecocounter.open_data_15min_counts AS eco
JOIN open_data.cycling_permanent_counts_locations AS od
    ON eco.site_description::text = od.location_name
    AND eco.direction::text = od.direction;

ALTER TABLE IF EXISTS open_data.cycling_permanent_counts_15min OWNER TO od_admins;

REVOKE ALL ON TABLE open_data.cycling_permanent_counts_15min FROM bdit_humans;
GRANT SELECT ON TABLE open_data.cycling_permanent_counts_15min TO bdit_humans;

GRANT SELECT ON TABLE open_data.cycling_permanent_counts_15min TO ecocounter_bot;

COMMENT ON VIEW open_data.cycling_permanent_counts_15min IS
'15 minute cycling volumes from various (currently just Eco-Counter) permanent counting stations.';