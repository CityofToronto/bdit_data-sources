-- Table: gwolofs.congestion_raw_segments

-- DROP TABLE IF EXISTS gwolofs.congestion_raw_segments;

CREATE TABLE IF NOT EXISTS gwolofs.congestion_raw_segments
(
    segment_id integer NOT NULL,
    dt date NOT NULL,
    bin_start timestamp without time zone NOT NULL,
    bin_range tsrange NOT NULL,
    tt numeric,
    num_obs integer,
    hr timestamp without time zone,
    CONSTRAINT congestion_raw_segments_pkey PRIMARY KEY (segment_id, dt, bin_start)
) PARTITION BY RANGE (dt);

ALTER TABLE IF EXISTS gwolofs.congestion_raw_segments
OWNER TO gwolofs;

REVOKE ALL ON TABLE gwolofs.congestion_raw_segments FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.congestion_raw_segments TO bdit_humans;

GRANT ALL ON TABLE gwolofs.congestion_raw_segments TO gwolofs;
-- Index: congestion_raw_segments_dt_idx

-- DROP INDEX IF EXISTS gwolofs.congestion_raw_segments_dt_idx;

CREATE INDEX IF NOT EXISTS congestion_raw_segments_dt_idx
ON gwolofs.congestion_raw_segments USING brin
(dt);
-- Index: congestion_raw_segments_segment_dt_idx

-- DROP INDEX IF EXISTS gwolofs.congestion_raw_segments_segment_dt_idx;

CREATE INDEX IF NOT EXISTS congestion_raw_segments_segment_dt_idx
ON gwolofs.congestion_raw_segments USING btree
(segment_id ASC NULLS LAST, bin_start ASC NULLS LAST);

-- Partitions SQL

CREATE TABLE gwolofs.congestion_raw_segments_2023 PARTITION OF gwolofs.congestion_raw_segments
FOR VALUES FROM ('2023-01-01') TO ('2024-01-01')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.congestion_raw_segments_2023
OWNER TO gwolofs;

CREATE TABLE gwolofs.congestion_raw_segments_2024 PARTITION OF gwolofs.congestion_raw_segments
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.congestion_raw_segments_2024
OWNER TO gwolofs;

CREATE TABLE gwolofs.congestion_raw_segments_2025 PARTITION OF gwolofs.congestion_raw_segments
FOR VALUES FROM ('2025-01-01') TO ('2026-01-01')
TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.congestion_raw_segments_2025
OWNER TO gwolofs;

COMMENT ON COLUMN gwolofs.congestion_raw_segments.dt
    IS 'The date of aggregation for the record. Records may not overlap dates.';

COMMENT ON COLUMN gwolofs.congestion_raw_segments.bin_start
    IS 'The start of the observation. It is recommended to use `hr` to group the bin instead. This column is used in the primary key, although the main constraint occurs during insert (non overlapping ranges).';

COMMENT ON COLUMN gwolofs.congestion_raw_segments.bin_range
    IS 'Bin range. An exclusion constraint on a temp table prevents overlapping ranges during insert.';

COMMENT ON COLUMN gwolofs.congestion_raw_segments.tt
    IS 'Travel time in seconds.';

COMMENT ON COLUMN gwolofs.congestion_raw_segments.num_obs
    IS 'The sum of the sample size from here.ta_path.';

COMMENT ON COLUMN gwolofs.congestion_raw_segments.hr
    IS 'The hour the majority of the record occured in. Ties are rounded up.';
