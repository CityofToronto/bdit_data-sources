-- Table: ecocounter.open_data_daily_counts

-- DROP TABLE IF EXISTS ecocounter.open_data_daily_counts;

CREATE TABLE IF NOT EXISTS ecocounter.open_data_daily_counts
(
    site_id numeric,
    site_description text COLLATE pg_catalog."default",
    direction travel_directions,
    dt date,
    daily_volume numeric,
    CONSTRAINT eco_open_data_daily_pkey PRIMARY KEY (site_id, direction, dt)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS ecocounter.open_data_daily_counts OWNER TO ecocounter_admins;

REVOKE ALL ON TABLE ecocounter.open_data_daily_counts FROM bdit_humans;
GRANT SELECT ON TABLE ecocounter.open_data_daily_counts TO bdit_humans;

GRANT SELECT, INSERT ON TABLE ecocounter.open_data_daily_counts TO ecocounter_bot;