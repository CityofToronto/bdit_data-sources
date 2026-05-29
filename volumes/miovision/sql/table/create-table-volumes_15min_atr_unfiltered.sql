-- Table: miovision_api.volumes_15min_atr_unfiltered_table

-- DROP TABLE IF EXISTS miovision_api.volumes_15min_atr_unfiltered_table;

CREATE TABLE IF NOT EXISTS miovision_api.volumes_15min_atr_unfiltered_table
(
    intersection_uid integer,
    datetime_bin timestamp without time zone,
    classification_uid integer,
    leg text COLLATE pg_catalog."default",
    dir text COLLATE pg_catalog."default",
    volume integer
)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.volumes_15min_atr_unfiltered_table
OWNER TO miovision_admins;

REVOKE ALL ON TABLE miovision_api.volumes_15min_atr_unfiltered_table FROM bdit_humans;

GRANT SELECT ON TABLE miovision_api.volumes_15min_atr_unfiltered_table TO bdit_humans;

GRANT ALL ON TABLE miovision_api.volumes_15min_atr_unfiltered_table TO miovision_admins;

COMMENT ON TABLE miovision_api.volumes_15min_atr_unfiltered_table
IS '(IN DEVELOPMENT) ATR Table to improve ATR query speeds.';
CREATE INDEX IF NOT EXISTS volumes_15min_atr_unfiltered_intersection_uid_idx
ON miovision_api.volumes_15min_atr_unfiltered_table USING btree
(intersection_uid ASC NULLS LAST)
WITH (fillfactor=100, deduplicate_items=True)
TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.volumes_15min_atr_unfiltered_table
ADD CONSTRAINT volumes_15min_atr_unfiltered_int_dt_bin_class_leg_mvmt_uid_pkey
PRIMARY KEY (intersection_uid, datetime_bin, classification_uid, leg, dir);

CREATE INDEX IF NOT EXISTS volumes_15min_atr_unfiltered_datetime_bin_idx
ON miovision_api.volumes_15min_atr_unfiltered_table USING brin
(datetime_bin)
WITH (pages_per_range=128, autosummarize=False)
TABLESPACE pg_default;
