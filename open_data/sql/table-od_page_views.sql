-- Table: open_data.od_page_views

-- DROP TABLE IF EXISTS open_data.od_page_views;

CREATE TABLE IF NOT EXISTS open_data.od_page_views
(
    page_id text COLLATE pg_catalog."default" NOT NULL,
    mnth date NOT NULL,
    sessions integer,
    users integer,
    views integer,
    avg_session_duration numeric,
    CONSTRAINT od_page_views_pkey PRIMARY KEY (page_id, mnth)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS open_data.od_page_views
OWNER TO od_admins;

REVOKE ALL ON TABLE open_data.od_page_views FROM bdit_humans;

GRANT SELECT, TRIGGER, REFERENCES ON TABLE open_data.od_page_views TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE open_data.od_page_views TO od_admins;

GRANT ALL ON TABLE open_data.od_page_views TO rds_superuser WITH GRANT OPTION;

REVOKE ALL ON TABLE open_data.od_page_views FROM ref_bot;
GRANT INSERT, SELECT ON TABLE open_data.od_page_views TO ref_bot;

COMMENT ON TABLE open_data.od_page_views
IS 'Stores a list of Open Data Page Views from https://open.toronto.ca/dataset/open-data-web-analytics/.
Updated Monthly by Airflow open_data_checks.';
