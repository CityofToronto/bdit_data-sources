-- Table: ecocounter.open_data_15min_counts

-- DROP TABLE IF EXISTS ecocounter.open_data_15min_counts;

CREATE TABLE IF NOT EXISTS ecocounter.open_data_15min_counts
(
    site_id numeric,
    site_description text COLLATE pg_catalog."default",
    direction travel_directions,
    datetime_bin timestamp without time zone,
    bin_volume numeric,
    CONSTRAINT eco_open_data_raw_pkey PRIMARY KEY (site_id, direction, datetime_bin)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS ecocounter.open_data_15min_counts OWNER TO ecocounter_admins;

REVOKE ALL ON TABLE ecocounter.open_data_15min_counts FROM bdit_humans;
GRANT SELECT ON TABLE ecocounter.open_data_15min_counts TO bdit_humans;

GRANT SELECT, INSERT ON TABLE ecocounter.open_data_15min_counts TO ecocounter_bot;

COMMENT ON TABLE ecocounter.open_data_15min_counts IS
'Disaggregate Ecocounter data by site and direction.';