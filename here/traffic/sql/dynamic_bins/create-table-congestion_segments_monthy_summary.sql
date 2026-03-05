-- Table: here_agg.segments_monthy_summary

-- DROP TABLE IF EXISTS here_agg.segments_monthy_summary;

CREATE TABLE IF NOT EXISTS here_agg.segments_monthy_summary
(
    segment_id integer,
    mnth date,
    is_wkdy boolean,
    hr smallint,
    avg_tt real,
    stdev real,
    percentile_05 real,
    percentile_15 real,
    percentile_50 real,
    percentile_85 real,
    percentile_95 real,
    num_quasi_obs smallint
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here_agg.segments_monthy_summary
OWNER TO here_admins;

REVOKE ALL ON TABLE here_agg.segments_monthy_summary FROM bdit_humans;

GRANT SELECT ON TABLE here_agg.segments_monthy_summary TO bdit_humans;
