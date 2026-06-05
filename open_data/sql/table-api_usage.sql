-- Table: open_data.od_api_usage

-- DROP TABLE IF EXISTS open_data.od_api_usage;

CREATE TABLE IF NOT EXISTS open_data.od_api_usage
(
    _id bigint NOT NULL,
    resource_id text COLLATE pg_catalog."default",
    dt date,
    uri text COLLATE pg_catalog."default",
    count integer,
    CONSTRAINT od_api_usage_pkey PRIMARY KEY (_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS open_data.od_api_usage
OWNER TO od_admins;

REVOKE ALL ON TABLE open_data.od_api_usage FROM bdit_humans;
REVOKE ALL ON TABLE open_data.od_api_usage FROM ref_bot;

GRANT SELECT, TRIGGER, REFERENCES ON TABLE open_data.od_api_usage TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE open_data.od_api_usage TO od_admins;

GRANT ALL ON TABLE open_data.od_api_usage TO rds_superuser WITH GRANT OPTION;

GRANT INSERT, SELECT ON TABLE open_data.od_api_usage TO ref_bot;

COMMENT ON TABLE open_data.od_api_usage
IS 'Stores a list of Open Data API Usage from https://open.toronto.ca/dataset/open-data-web-analytics/.
Updated Monthly by Airflow open_data_checks.';
