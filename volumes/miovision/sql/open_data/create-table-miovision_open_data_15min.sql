-- DROP TABLE IF EXISTS gwolofs.miovision_15min_open_data;

CREATE TABLE IF NOT EXISTS gwolofs.miovision_15min_open_data
(
    intersection_uid integer NOT NULL,
    intersection_long_name text COLLATE pg_catalog."default",
    datetime_15min timestamp without time zone NOT NULL,
    classification_type text COLLATE pg_catalog."default" NOT NULL,
    entry_leg text COLLATE pg_catalog."default" NOT NULL,
    entry_dir text COLLATE pg_catalog."default",
    movement text COLLATE pg_catalog."default" NOT NULL,
    exit_leg text COLLATE pg_catalog."default",
    exit_dir text COLLATE pg_catalog."default",
    volume_15min smallint,
    CONSTRAINT miovision_open_data_15min_pkey PRIMARY KEY (
        intersection_uid, datetime_15min, classification_type, entry_leg, movement
    )
)

TABLESPACE pg_default;

CREATE INDEX miovision_15min_od_dt_idx ON
gwolofs.miovision_15min_open_data USING brin (datetime_15min);

ALTER TABLE IF EXISTS gwolofs.miovision_15min_open_data
OWNER TO gwolofs;

REVOKE ALL ON TABLE gwolofs.miovision_15min_open_data FROM bdit_humans;

GRANT SELECT ON TABLE gwolofs.miovision_15min_open_data TO bdit_humans;

GRANT ALL ON TABLE gwolofs.miovision_15min_open_data TO gwolofs;

GRANT ALL ON TABLE gwolofs.miovision_15min_open_data TO miovision_admins;

GRANT SELECT, INSERT, DELETE ON TABLE gwolofs.miovision_15min_open_data
TO miovision_api_bot;

COMMENT ON TABLE gwolofs.miovision_15min_open_data
IS 'Table to store Miovision 15min open data. Updated monthly. 
Schema is a blend of TMC and ATR style data to cover different
types of data requests.';