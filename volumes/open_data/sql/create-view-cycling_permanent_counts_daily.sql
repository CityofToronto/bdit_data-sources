-- View: open_data.cycling_permanent_counts_daily

-- DROP VIEW IF EXISTS open_data.cycling_permanent_counts_daily;

CREATE OR REPLACE VIEW open_data.cycling_permanent_counts_daily AS
SELECT
    od.location_dir_id,
    eco.site_description::text AS location_name,
    eco.direction::text AS direction,
    od.linear_name_full,
    od.side_street,
    eco.dt::date,
    eco.daily_volume::integer
FROM ecocounter.open_data_daily_counts AS eco
JOIN open_data.cycling_permanent_counts_locations AS od
    ON eco.site_description::text = od.location_name
    AND eco.direction::text = od.direction;

ALTER TABLE IF EXISTS open_data.cycling_permanent_counts_daily OWNER TO od_admins;

REVOKE ALL ON TABLE open_data.cycling_permanent_counts_daily FROM bdit_humans;
GRANT SELECT ON TABLE open_data.cycling_permanent_counts_daily TO bdit_humans;

GRANT SELECT ON TABLE open_data.cycling_permanent_counts_daily TO ecocounter_bot;

COMMENT ON VIEW open_data.cycling_permanent_counts_daily IS
'Daily cycling volumes from various (currently just Eco-Counter) permanent counting stations.';