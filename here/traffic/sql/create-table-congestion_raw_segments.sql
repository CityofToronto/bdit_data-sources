-- Table: gwolofs.congestion_raw_segments

-- DROP TABLE IF EXISTS gwolofs.congestion_raw_segments;

CREATE TABLE IF NOT EXISTS gwolofs.congestion_raw_segments
(
    time_grp timestamp without time zone NOT NULL,
    segment_id integer NOT NULL,
    bin_range tsrange NOT NULL,
    dt_start timestamp without time zone,
    dt_end timestamp without time zone,
    tt numeric,
    unadjusted_tt numeric,
    total_length numeric,
    length_w_data numeric,
    num_obs integer,
    CONSTRAINT dynamic_bins_unique EXCLUDE USING gist (
        segment_id WITH =,
        bin_range WITH &&,
        time_grp WITH =
    )
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.congestion_raw_segments
OWNER TO gwolofs;

REVOKE ALL ON TABLE gwolofs.congestion_raw_segments FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.congestion_raw_segments TO bdit_humans;

GRANT ALL ON TABLE gwolofs.congestion_raw_segments TO gwolofs;
-- Index: dynamic_bin_idx

-- DROP INDEX IF EXISTS gwolofs.dynamic_bin_idx;

CREATE INDEX IF NOT EXISTS dynamic_bin_idx
    ON gwolofs.congestion_raw_segments USING btree
    (segment_id ASC NULLS LAST, time_grp ASC NULLS LAST)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;