-- Table: gwolofs.congestion_raw_corridors

-- DROP TABLE IF EXISTS gwolofs.congestion_raw_corridors;

CREATE TABLE IF NOT EXISTS gwolofs.congestion_raw_corridors
(
    time_grp tsrange NOT NULL,
    bin_range tsrange NOT NULL,
    dt_start timestamp without time zone,
    dt_end timestamp without time zone,
    tt numeric,
    unadjusted_tt numeric,
    total_length numeric,
    length_w_data numeric,
    num_obs integer,
    segment_uid smallint,
    uri_string text COLLATE pg_catalog."default",
    CONSTRAINT congestion_raw_corridors_exclude EXCLUDE USING gist (
        bin_range WITH &&,
        segment_uid WITH =,
        time_grp WITH =,
        uri_string WITH =)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.congestion_raw_corridors
    OWNER to gwolofs;

REVOKE ALL ON TABLE gwolofs.congestion_raw_corridors FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.congestion_raw_corridors TO bdit_humans;

GRANT ALL ON TABLE gwolofs.congestion_raw_corridors TO gwolofs;

-- Index: dynamic_binning_results_time_grp_segment_uid_idx

-- DROP INDEX IF EXISTS gwolofs.congestion_raw_corridors_time_grp_segment_uid_idx;

CREATE INDEX IF NOT EXISTS dynamic_binning_results_time_grp_segment_uid_idx
    ON gwolofs.congestion_raw_corridors USING btree
    (time_grp ASC NULLS LAST, segment_uid ASC NULLS LAST)
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;