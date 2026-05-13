-- Table: here.street_valid_range_path_hm

-- DROP TABLE IF EXISTS here.street_valid_range_path_hm;

CREATE TABLE IF NOT EXISTS here.street_valid_range_path_hm
(
    street_version text COLLATE pg_catalog."default",
    valid_range daterange
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here.street_valid_range_path_hm
    OWNER to here_admins;

REVOKE ALL ON TABLE here.street_valid_range_path_hm FROM bdit_humans;
REVOKE ALL ON TABLE here.street_valid_range_path_hm FROM congestion_bot;
REVOKE ALL ON TABLE here.street_valid_range_path_hm FROM covid_admins;
REVOKE ALL ON TABLE here.street_valid_range_path_hm FROM here_bot;
REVOKE ALL ON TABLE here.street_valid_range_path_hm FROM tt_request_bot;

GRANT SELECT ON TABLE here.street_valid_range_path_hm TO bdit_humans;

GRANT SELECT ON TABLE here.street_valid_range_path_hm TO congestion_bot;

GRANT SELECT ON TABLE here.street_valid_range_path_hm TO covid_admins;

GRANT ALL ON TABLE here.street_valid_range_path_hm TO here_admins;

GRANT SELECT ON TABLE here.street_valid_range_path_hm TO here_bot;

GRANT SELECT ON TABLE here.street_valid_range_path_hm TO tt_request_bot;

COMMENT ON TABLE here.street_valid_range_path_hm
IS 'Table that documents which street version here.ta_path_hm''s data is based on for specific date range. 
This table gets update every year when we update our here map version. ';
