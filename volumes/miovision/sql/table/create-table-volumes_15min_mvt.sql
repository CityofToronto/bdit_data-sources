CREATE TABLE miovision_api.volumes_15min_mvt
(
    volume_15min_mvt_uid integer NOT NULL DEFAULT nextval('miovision_api.volumes_15min_mvt_volume_15min_mvt_uid_seq'::regclass),
    intersection_uid integer,
    datetime_bin timestamp without time zone,
    classification_uid integer,
    leg text,
    movement_uid integer,
    volume numeric,
    processed boolean,
    CONSTRAINT volumes_15min_mvt_int_dt_bin_class_leg_mvmt_uid_pkey
    PRIMARY KEY (
        intersection_uid, datetime_bin, classification_uid, leg, movement_uid
    )
)
PARTITION BY RANGE (datetime_bin)
WITH (
    OIDS = False
);

ALTER TABLE miovision_api.volumes_15min_mvt OWNER TO miovision_admins;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.volumes_15min_mvt
TO bdit_humans WITH GRANT OPTION;
GRANT SELECT, INSERT, TRIGGER ON TABLE miovision_api.volumes_15min_mvt TO miovision_api_bot;

-- Index: miovision_api.volumes_15min_mvt_classification_uid_idx
-- DROP INDEX miovision_api.volumes_15min_mvt_classification_uid_idx;
CREATE INDEX volumes_15min_mvt_classification_uid_idx
ON miovision_api.volumes_15min_mvt
USING btree (classification_uid);

-- Index: miovision_api.volumes_15min_mvt_datetime_bin_idx
-- DROP INDEX miovision_api.volumes_15min_mvt_datetime_bin_idx;
CREATE INDEX volumes_15min_mvt_datetime_bin_idx
ON miovision_api.volumes_15min_mvt
USING btree (datetime_bin);

-- Index: miovision_api.volumes_15min_mvt_intersection_uid_idx
-- DROP INDEX miovision_api.volumes_15min_mvt_intersection_uid_idx;
CREATE INDEX volumes_15min_mvt_intersection_uid_idx
ON miovision_api.volumes_15min_mvt
USING btree (intersection_uid);

-- Index: miovision_api.volumes_15min_mvt_leg_movement_uid_idx
-- DROP INDEX miovision_api.volumes_15min_mvt_leg_movement_uid_idx;
CREATE INDEX volumes_15min_mvt_leg_movement_uid_idx
ON miovision_api.volumes_15min_mvt
USING btree (
    leg COLLATE pg_catalog."default",
    movement_uid
);

-- Index: miovision_api.volumes_15min_mvt_volume_15min_mvt_uid_idx
-- DROP INDEX miovision_api.volumes_15min_mvt_volume_15min_mvt_uid_idx;
CREATE INDEX volumes_15min_mvt_volume_15min_mvt_uid_idx
ON miovision_api.volumes_15min_mvt
USING btree (volume_15min_mvt_uid ASC NULLS LAST);

-- Index: volumes_15min_mvt_processed_idx
-- DROP INDEX IF EXISTS miovision_api.volumes_15min_mvt_processed_idx;
CREATE INDEX IF NOT EXISTS volumes_15min_mvt_processed_idx
ON miovision_api.volumes_15min_mvt USING btree (processed ASC NULLS LAST)
WHERE processed IS NULL;

--create yearly partitions
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt', 2019::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt', 2020::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt', 2021::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt', 2022::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt', 2023::int);
