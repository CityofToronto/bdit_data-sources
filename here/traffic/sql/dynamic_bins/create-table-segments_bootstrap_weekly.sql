-- Table: here_agg.segments_bootstrap_weekly

-- DROP TABLE IF EXISTS here_agg.segments_bootstrap_weekly;

CREATE TABLE IF NOT EXISTS here_agg.segments_bootstrap_weekly
(
    segment_id bigint NOT NULL,
    dow_group text COLLATE pg_catalog."default" NOT NULL,
    week_start date NOT NULL,
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
    CONSTRAINT here_agg_weekly_segments_bootstrap_pkey PRIMARY KEY (segment_id, week_start, dow_group, hr_start, hr_end),
    CONSTRAINT week_start_check CHECK (date_part('isodow'::text, week_start) = 6::double precision)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here_agg.segments_bootstrap_weekly
    OWNER to here_admins;

REVOKE ALL ON TABLE here_agg.segments_bootstrap_weekly FROM bdit_humans;
REVOKE ALL ON TABLE here_agg.segments_bootstrap_weekly FROM congestion_bot;

GRANT SELECT, TRIGGER, REFERENCES ON TABLE here_agg.segments_bootstrap_weekly TO bdit_humans WITH GRANT OPTION;

GRANT INSERT, DELETE, SELECT ON TABLE here_agg.segments_bootstrap_weekly TO congestion_bot;

GRANT ALL ON TABLE here_agg.segments_bootstrap_weekly TO dbadmin;

GRANT ALL ON TABLE here_agg.segments_bootstrap_weekly TO here_admins;

GRANT ALL ON TABLE here_agg.segments_bootstrap_weekly TO rds_superuser WITH GRANT OPTION;