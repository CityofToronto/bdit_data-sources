CREATE TABLE IF NOT EXISTS here.street_valid_range_path
(
    street_version text COLLATE pg_catalog."default",
    valid_range daterange
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS here.street_valid_range_path
    OWNER to here_admins;


GRANT SELECT ON TABLE here.street_valid_range_path TO bdit_humans;

GRANT SELECT ON TABLE here.street_valid_range_path TO congestion_bot;

GRANT ALL ON TABLE here.street_valid_range_path TO here_admins;

GRANT SELECT ON TABLE here.street_valid_range_path TO tt_request_bot;

COMMENT ON TABLE here.street_valid_range_path
    IS 'Table that documents which street version here.ta_path''s data is based on for specific date range. 
This table gets update every year when we update our here map version. ';
