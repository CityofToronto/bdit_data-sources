-- Table: here.ta_path_daily_summary

-- DROP TABLE IF EXISTS here.ta_path_daily_summary;

CREATE TABLE IF NOT EXISTS here.ta_path_daily_summary
(
    dt date NOT NULL,
    row_count bigint,
    avg_speed numeric,
    sum_sample_size bigint,
    num_link_dirs bigint,
    CONSTRAINT ta_path_daily_summary_pkey PRIMARY KEY (dt)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here.ta_path_daily_summary OWNER TO here_admins;

REVOKE ALL ON TABLE here.ta_path_daily_summary FROM bdit_humans;
REVOKE ALL ON TABLE here.ta_path_daily_summary FROM here_bot;

GRANT SELECT ON TABLE here.ta_path_daily_summary TO bdit_humans;

GRANT ALL ON TABLE here.ta_path_daily_summary TO here_admins;

GRANT INSERT, SELECT, UPDATE ON TABLE here.ta_path_daily_summary TO here_bot;
