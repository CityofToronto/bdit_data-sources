-- Table: bluetooth.itsc_tt_raw_pathdata

-- DROP TABLE IF EXISTS bluetooth.itsc_tt_raw_pathdata;

CREATE TABLE IF NOT EXISTS bluetooth.itsc_tt_raw_pathdata
(
    division_id smallint,
    path_id integer NOT NULL,
    dt timestamp without time zone NOT NULL,
    travel_time_s integer,
    quality_metric double precision,
    num_samples integer,
    congestion_start_meters double precision,
    congestion_end_meters double precision,
    min_speed_kmh double precision,
    unmatched integer,
    fifth_percentile_tt_s integer,
    ninty_fifth_percentile_tt_s integer,
    CONSTRAINT tt_raw_pathdata_path_dt_new_pkey PRIMARY KEY (path_id, dt)
) PARTITION BY RANGE (dt);

ALTER TABLE IF EXISTS bluetooth.itsc_tt_raw_pathdata
OWNER TO bt_admins;

REVOKE ALL ON TABLE bluetooth.itsc_tt_raw_pathdata FROM bdit_humans;
REVOKE ALL ON TABLE bluetooth.itsc_tt_raw_pathdata FROM bt_insert_bot;

GRANT SELECT ON TABLE bluetooth.itsc_tt_raw_pathdata TO bdit_humans;

GRANT ALL ON TABLE bluetooth.itsc_tt_raw_pathdata TO bt_admins;

GRANT INSERT, SELECT, UPDATE ON TABLE bluetooth.itsc_tt_raw_pathdata TO bt_insert_bot;

GRANT ALL ON TABLE bluetooth.itsc_tt_raw_pathdata TO events_bot;

GRANT ALL ON TABLE bluetooth.itsc_tt_raw_pathdata TO rds_superuser WITH GRANT OPTION;
-- Index: tt_raw_pathdata_path_dt__idx

-- DROP INDEX IF EXISTS bluetooth.tt_raw_pathdata_path_dt__idx;

CREATE UNIQUE INDEX IF NOT EXISTS tt_raw_pathdata_path_dt__idx
    ON bluetooth.itsc_tt_raw_pathdata USING btree
    (path_id ASC NULLS LAST, dt ASC NULLS LAST);

-- Partitions SQL

CREATE TABLE bluetooth.itsc_tt_raw_pathdata_2024 PARTITION OF bluetooth.itsc_tt_raw_pathdata
FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2025-01-01 00:00:00')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS bluetooth.itsc_tt_raw_pathdata_2024
OWNER TO bt_admins;

CREATE TABLE bluetooth.itsc_tt_raw_pathdata_2025 PARTITION OF bluetooth.itsc_tt_raw_pathdata
FOR VALUES FROM ('2025-01-01 00:00:00') TO ('2026-01-01 00:00:00')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS bluetooth.itsc_tt_raw_pathdata_2025
OWNER TO bt_admins;
