-- Table: here.ta_path_daily_summary_link_dir

-- DROP TABLE IF EXISTS here.ta_path_daily_summary_link_dir;

CREATE TABLE IF NOT EXISTS here.ta_path_daily_summary_link_dir
(
    link_dir text COLLATE pg_catalog."default" NOT NULL,
    dts date [],
    row_counts bigint [],
    sum_sample_size bigint [],
    CONSTRAINT ta_path_daily_summary_link_dir_pkey PRIMARY KEY (link_dir)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here.ta_path_daily_summary_link_dir
OWNER TO here_admins;

REVOKE ALL ON TABLE here.ta_path_daily_summary_link_dir FROM bdit_humans;
REVOKE ALL ON TABLE here.ta_path_daily_summary_link_dir FROM here_bot;

GRANT SELECT ON TABLE here.ta_path_daily_summary_link_dir TO bdit_humans;

GRANT ALL ON TABLE here.ta_path_daily_summary_link_dir TO here_admins;

GRANT INSERT, SELECT, UPDATE ON TABLE here.ta_path_daily_summary_link_dir TO here_bot;
