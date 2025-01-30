-- Table: gwolofs.dynamic_binning_results

-- DROP TABLE IF EXISTS gwolofs.dynamic_binning_results;

CREATE TABLE IF NOT EXISTS gwolofs.dynamic_binning_results
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
    CONSTRAINT dynamic_bins_unique_temp EXCLUDE USING gist (
        bin_range WITH &&,
        time_grp WITH =,
        segment_uid WITH =,
        uri_string WITH =
    )

)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.dynamic_binning_results
    OWNER TO gwolofs;

REVOKE ALL ON TABLE gwolofs.dynamic_binning_results FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.dynamic_binning_results TO bdit_humans;

GRANT ALL ON TABLE gwolofs.dynamic_binning_results TO gwolofs;
-- Index: dynamic_binning_results_time_grp_segment_uid_idx

-- DROP INDEX IF EXISTS gwolofs.dynamic_binning_results_time_grp_segment_uid_idx;

CREATE INDEX IF NOT EXISTS dynamic_binning_results_time_grp_segment_uid_idx
    ON gwolofs.dynamic_binning_results USING btree
    (
        time_grp ASC NULLS LAST,
        segment_uid ASC NULLS LAST
    )
    WITH (deduplicate_items=True)
    TABLESPACE pg_default;