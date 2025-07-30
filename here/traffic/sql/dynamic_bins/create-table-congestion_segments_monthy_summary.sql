-- Table: gwolofs.congestion_segments_monthy_summary

-- DROP TABLE IF EXISTS gwolofs.congestion_segments_monthy_summary;

CREATE TABLE IF NOT EXISTS gwolofs.congestion_segments_monthy_summary
(
    segment_id integer,
    mnth date,
    is_wkdy boolean,
    hr double precision,
    avg_tt numeric,
    stdev numeric,
    percentile_05 numeric,
    percentile_15 numeric,
    percentile_50 numeric,
    percentile_85 numeric,
    percentile_95 numeric,
    num_quasi_obs bigint
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.congestion_segments_monthy_summary
OWNER TO gwolofs;

REVOKE ALL ON TABLE gwolofs.congestion_segments_monthy_summary FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.congestion_segments_monthy_summary TO bdit_humans;

GRANT ALL ON TABLE gwolofs.congestion_segments_monthy_summary TO gwolofs;
