-- Table: open_data.od_resources

-- DROP TABLE IF EXISTS open_data.od_resources;

CREATE TABLE IF NOT EXISTS open_data.od_resources
(
    page_id text COLLATE pg_catalog."default" NOT NULL,
    resource_name text COLLATE pg_catalog."default" NOT NULL,
    resource_id text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT od_resources_pkey PRIMARY KEY (page_id, resource_name, resource_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS open_data.od_resources
OWNER TO od_admins;

REVOKE ALL ON TABLE open_data.od_resources FROM bdit_humans;

GRANT SELECT, TRIGGER, REFERENCES ON TABLE open_data.od_resources TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE open_data.od_resources TO od_admins;

GRANT INSERT, SELECT ON TABLE open_data.od_resources TO ref_bot;

GRANT ALL ON TABLE open_data.od_resources TO rds_superuser WITH GRANT OPTION;

COMMENT ON TABLE open_data.od_resources
IS 'Stores a list of our Open Data resources. Updated Monthly by Airflow open_data_checks.';
