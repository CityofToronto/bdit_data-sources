-- Table: here_agg.segments_bootstrap_monthly

-- DROP TABLE IF EXISTS here_agg.segments_bootstrap_monthly;

CREATE TABLE IF NOT EXISTS here_agg.segments_bootstrap_monthly
(
    segment_id bigint NOT NULL,
    dow_group text COLLATE pg_catalog."default" NOT NULL,
    mnth date NOT NULL,
    holiday_exceptions date[],
    hr_start smallint NOT NULL,
    hr_end smallint NOT NULL,
    avg_tt real,
    avg_ci_lower real,
    avg_ci_upper real,
    q1_tt real,
    q1_ci_lower real,
    q1_ci_upper real,
    median_tt real,
    median_ci_lower real,
    median_ci_upper real,
    q3_tt real,
    q3_ci_lower real,
    q3_ci_upper real,
    n integer,
    n_resample integer,
    length numeric,
    CONSTRAINT congestion_segments_monthly_bootstrap_pkey PRIMARY KEY (segment_id, dow_group, mnth, hr_start, hr_end),
    CONSTRAINT segments_bootstrap_monthly_mnth_check CHECK (date_part('day'::text, mnth) = 1::double precision)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here_agg.segments_bootstrap_monthly
    OWNER to here_admins;

REVOKE ALL ON TABLE here_agg.segments_bootstrap_monthly FROM bdit_humans;
REVOKE ALL ON TABLE here_agg.segments_bootstrap_monthly FROM congestion_bot;

GRANT SELECT, TRIGGER, REFERENCES ON TABLE here_agg.segments_bootstrap_monthly TO bdit_humans WITH GRANT OPTION;

GRANT INSERT, DELETE, SELECT ON TABLE here_agg.segments_bootstrap_monthly TO congestion_bot;

GRANT ALL ON TABLE here_agg.segments_bootstrap_monthly TO dbadmin;

GRANT ALL ON TABLE here_agg.segments_bootstrap_monthly TO here_admins;

GRANT ALL ON TABLE here_agg.segments_bootstrap_monthly TO rds_superuser WITH GRANT OPTION;