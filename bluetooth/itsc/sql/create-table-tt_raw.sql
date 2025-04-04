-- Table: bluetooth.itsc_tt_raw

-- DROP TABLE IF EXISTS bluetooth.itsc_tt_raw;

CREATE TABLE IF NOT EXISTS bluetooth.itsc_tt_raw
(
    division_id smallint,
    path_id integer NOT NULL,
    raw_data_type integer,
    dt timestamp without time zone NOT NULL,
    travel_time_s integer,
    quality_metric double precision,
    num_samples integer,
    congestion_start_meters double precision,
    congestion_end_meters double precision,
    min_speed_kmh double precision,
    fifth_percentile_tt_s integer,
    nintyfifth_percentile_tt_s integer,
    unmatched integer,
    CONSTRAINT tt_raw_path_dt_parent_pkey PRIMARY KEY (path_id, dt)
) PARTITION BY RANGE (dt);

ALTER TABLE IF EXISTS bluetooth.itsc_tt_raw
OWNER TO bt_admins;

REVOKE ALL ON TABLE bluetooth.itsc_tt_raw FROM bdit_humans;
REVOKE ALL ON TABLE bluetooth.itsc_tt_raw FROM bt_insert_bot;

GRANT SELECT ON TABLE bluetooth.itsc_tt_raw TO bdit_humans;

GRANT ALL ON TABLE bluetooth.itsc_tt_raw TO bt_admins;

GRANT INSERT, SELECT, UPDATE ON TABLE bluetooth.itsc_tt_raw TO bt_insert_bot;

GRANT ALL ON TABLE bluetooth.itsc_tt_raw TO events_bot;

GRANT ALL ON TABLE bluetooth.itsc_tt_raw TO rds_superuser WITH GRANT OPTION;

-- Partitions SQL
CREATE TABLE bluetooth.itsc_tt_raw_2024 PARTITION OF bluetooth.itsc_tt_raw
FOR VALUES FROM ('2024-01-01 00:00:00') TO ('2025-01-01 00:00:00')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS bluetooth.itsc_tt_raw_2024
OWNER TO bt_admins;

CREATE TABLE bluetooth.itsc_tt_raw_2025 PARTITION OF bluetooth.itsc_tt_raw
FOR VALUES FROM ('2025-01-01 00:00:00') TO ('2026-01-01 00:00:00')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS bluetooth.itsc_tt_raw_2025
OWNER TO bt_admins;

CREATE UNIQUE INDEX tt_raw_path_dt_idx
ON bluetooth.itsc_tt_raw USING btree(
    path_id, dt
);
