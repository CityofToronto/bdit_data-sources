-- Table: open_data.od_pages

-- DROP TABLE IF EXISTS open_data.od_pages;

CREATE TABLE IF NOT EXISTS open_data.od_pages
(
    page_id text COLLATE pg_catalog."default" NOT NULL,
    last_refreshed timestamp without time zone,
    refresh_rate text COLLATE pg_catalog."default",
    CONSTRAINT od_pages_pkey PRIMARY KEY (page_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS open_data.od_pages
OWNER TO od_admins;

REVOKE ALL ON TABLE open_data.od_pages FROM bdit_humans;
GRANT SELECT, TRIGGER, REFERENCES ON TABLE open_data.od_pages TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE open_data.od_pages TO od_admins;

GRANT ALL ON TABLE open_data.od_pages TO rds_superuser WITH GRANT OPTION;

REVOKE ALL ON TABLE open_data.od_pages FROM ref_bot;
GRANT INSERT, SELECT, UPDATE ON TABLE open_data.od_pages TO ref_bot;

COMMENT ON TABLE open_data.od_pages
IS 'List of Open Data pages maintained by Data & Analytics.';