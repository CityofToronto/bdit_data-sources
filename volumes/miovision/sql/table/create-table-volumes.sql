-- Table: miovision_api.volumes
-- DROP TABLE miovision_api.volumes;
CREATE TABLE miovision_api.volumes
(
    volume_uid bigserial NOT NULL,
    intersection_uid integer,
    datetime_bin timestamp without time zone,
    classification_uid integer,
    leg text,
    movement_uid integer,
    volume integer,
    volume_15min_mvt_uid integer,
    CONSTRAINT volumes_intersection_uid_datetime_bin_classification_pkey 
    PRIMARY KEY (intersection_uid, datetime_bin, classification_uid, leg, movement_uid)
)
PARTITION BY RANGE (datetime_bin)
WITH (
    OIDS=FALSE
);
ALTER TABLE miovision_api.volumes OWNER TO miovision_admins;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.volumes TO bdit_humans WITH GRANT OPTION;
GRANT SELECT, INSERT, UPDATE ON TABLE miovision_api.volumes TO miovision_api_bot;

-- Index: miovision_api.volumes_datetime_bin_idx
-- DROP INDEX miovision_api.volumes_datetime_bin_idx;
CREATE INDEX volumes_datetime_bin_idx
ON miovision_api.volumes
USING brin(datetime_bin);

-- Index: miovision_api.volumes_intersection_uid_classification_uid_leg_movement_ui_idx
-- DROP INDEX miovision_api.volumes_intersection_uid_classification_uid_leg_movement_ui_idx;
CREATE INDEX volumes_intersection_uid_classification_uid_leg_movement_ui_idx
ON miovision_api.volumes
USING btree(
    intersection_uid, classification_uid, leg COLLATE pg_catalog."default", movement_uid
);

-- Index: miovision_api.volumes_intersection_uid_idx
-- DROP INDEX miovision_api.volumes_intersection_uid_idx;
CREATE INDEX volumes_intersection_uid_idx
ON miovision_api.volumes
USING btree(intersection_uid);

-- Index: miovision_api.volumes_volume_15min_mvt_uid_idx
-- DROP INDEX miovision_api.volumes_volume_15min_mvt_uid_idx;
CREATE INDEX volumes_volume_15min_mvt_uid_idx
ON miovision_api.volumes
USING btree(volume_15min_mvt_uid);

--the old pkey becomes a unique constraint
-- Index: miovision_api.volume_uid_unique
-- DROP INDEX miovision_api.volume_uid_unique;
CREATE INDEX volume_uid_unique
ON miovision_api.volumes
USING btree(volume_uid);

ALTER TABLE miovision_api.volumes OWNER TO miovision_admins;
