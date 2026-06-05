-- Table: open_data.od_file_clicks

-- DROP TABLE IF EXISTS open_data.od_file_clicks;

CREATE TABLE IF NOT EXISTS open_data.od_file_clicks
(
    _id bigint NOT NULL,
    url text COLLATE pg_catalog."default" NOT NULL,
    clicks integer,
    mnth date NOT NULL,
    package_name text COLLATE pg_catalog."default",
    resource_name text COLLATE pg_catalog."default",
    CONSTRAINT od_file_clicks_pkey PRIMARY KEY (_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS open_data.od_file_clicks
OWNER TO od_admins;

REVOKE ALL ON TABLE open_data.od_file_clicks FROM bdit_humans;

GRANT SELECT, TRIGGER, REFERENCES ON TABLE open_data.od_file_clicks TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE open_data.od_file_clicks TO od_admins;

GRANT INSERT, SELECT ON TABLE open_data.od_file_clicks TO ref_bot;

GRANT ALL ON TABLE open_data.od_file_clicks TO rds_superuser WITH GRANT OPTION;

COMMENT ON TABLE open_data.od_file_clicks
IS 'Stores a list of Open Data File Clicks from https://open.toronto.ca/dataset/open-data-web-analytics/.
Updated Monthly by Airflow open_data_checks.';
