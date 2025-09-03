-- Table: gwolofs.congestion_segments_monthly_bootstrap

-- DROP TABLE IF EXISTS gwolofs.congestion_segments_monthly_bootstrap;

CREATE TABLE IF NOT EXISTS gwolofs.congestion_segments_monthly_bootstrap
(
    segment_id integer NOT NULL,
    mnth date NOT NULL,
    is_wkdy boolean NOT NULL,
    hr numeric NOT NULL,
    avg_tt real,
    n bigint,
    ci_lower real,
    ci_upper real,
    n_resamples integer NOT NULL,
    CONSTRAINT congestion_segments_monthly_bootstrap_pkey PRIMARY KEY (segment_id, mnth, is_wkdy, hr, n_resamples)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS gwolofs.congestion_segments_monthly_bootstrap
OWNER TO gwolofs;

REVOKE ALL ON TABLE gwolofs.congestion_segments_monthly_bootstrap FROM bdit_humans;
REVOKE ALL ON TABLE gwolofs.congestion_segments_monthly_bootstrap FROM congestion_bot;

GRANT SELECT, TRIGGER, REFERENCES ON TABLE gwolofs.congestion_segments_monthly_bootstrap TO bdit_humans WITH GRANT OPTION;

GRANT INSERT, SELECT, DELETE ON TABLE gwolofs.congestion_segments_monthly_bootstrap TO congestion_bot;

GRANT ALL ON TABLE gwolofs.congestion_segments_monthly_bootstrap TO dbadmin;

GRANT ALL ON TABLE gwolofs.congestion_segments_monthly_bootstrap TO rds_superuser WITH GRANT OPTION;
