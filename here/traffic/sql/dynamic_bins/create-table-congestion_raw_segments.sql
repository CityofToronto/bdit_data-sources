-- Table: gwolofs.congestion_raw_segments

-- DROP TABLE IF EXISTS gwolofs.congestion_raw_segments;

CREATE TABLE IF NOT EXISTS gwolofs.congestion_raw_segments
(
    segment_id integer NOT NULL,
    dt date NOT NULL,
    time_grp tsrange NOT NULL,
    bin_range tsrange NOT NULL,
    tt numeric,
    num_obs integer,
    CONSTRAINT congestion_raw_segments_exclude EXCLUDE USING gist (
        segment_id WITH =,
        dt WITH =,
        time_grp WITH =,
        bin_range WITH &&
    )
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.congestion_raw_segments
OWNER TO gwolofs;

REVOKE ALL ON TABLE gwolofs.congestion_raw_segments FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.congestion_raw_segments TO bdit_humans;

GRANT ALL ON TABLE gwolofs.congestion_raw_segments TO gwolofs;
-- Index: dynamic_bin_hr_idx

-- DROP INDEX IF EXISTS gwolofs.dynamic_bin_hr_idx;

CREATE INDEX IF NOT EXISTS dynamic_bin_dt_idx
    ON gwolofs.congestion_raw_segments USING btree
    (dt ASC NULLS LAST)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: dynamic_bin_idx

-- DROP INDEX IF EXISTS gwolofs.dynamic_bin_idx;

CREATE INDEX IF NOT EXISTS dynamic_bin_idx
    ON gwolofs.congestion_raw_segments USING btree
    (segment_id ASC NULLS LAST, dt ASC NULLS LAST)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;