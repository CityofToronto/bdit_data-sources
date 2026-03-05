-- Table: here_agg.segments_monthly_bootstrap

-- DROP TABLE IF EXISTS here_agg.segments_monthly_bootstrap;

CREATE TABLE IF NOT EXISTS here_agg.segments_monthly_bootstrap
(
    segment_id integer NOT NULL,
    mnth date NOT NULL,
    is_wkdy boolean NOT NULL,
    hr smallint NOT NULL,
    avg_tt real,
    n smallint,
    ci_lower real,
    ci_upper real,
    n_resamples smallint NOT NULL,
    CONSTRAINT congestion_segments_monthly_bootstrap_pkey PRIMARY KEY (segment_id, mnth, is_wkdy, hr, n_resamples)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here_agg.segments_monthly_bootstrap
OWNER TO here_admins;

REVOKE ALL ON TABLE here_agg.segments_monthly_bootstrap FROM bdit_humans;
REVOKE ALL ON TABLE here_agg.segments_monthly_bootstrap FROM congestion_bot;

GRANT SELECT, TRIGGER, REFERENCES ON TABLE here_agg.segments_monthly_bootstrap TO bdit_humans WITH GRANT OPTION;

GRANT INSERT, SELECT, DELETE ON TABLE here_agg.segments_monthly_bootstrap TO congestion_bot;

GRANT ALL ON TABLE here_agg.segments_monthly_bootstrap TO dbadmin;

GRANT ALL ON TABLE here_agg.segments_monthly_bootstrap TO rds_superuser WITH GRANT OPTION;
