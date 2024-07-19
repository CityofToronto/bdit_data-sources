-- Table: miovision_api.volumes_15min

-- DROP TABLE miovision_api.volumes_15min;
CREATE TABLE miovision_api.volumes_15min
(
    volume_15min_uid integer NOT NULL
    DEFAULT nextval('miovision_api.volumes_15min_volume_15min_uid_seq'::regclass),
    intersection_uid integer,
    datetime_bin timestamp without time zone,
    classification_uid integer,
    leg text,
    dir text,
    volume numeric,
    CONSTRAINT volumes_15min_intersection_uid_datetime_bin_classification_pkey
    PRIMARY KEY (intersection_uid, datetime_bin, classification_uid, leg, dir)
)
PARTITION BY RANGE (datetime_bin)
WITH (
    oids = FALSE
);

ALTER TABLE miovision_api.volumes_15min OWNER TO miovision_admins;
GRANT ALL ON TABLE miovision_api.volumes_15min TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.volumes_15min
TO bdit_humans WITH GRANT OPTION;
GRANT SELECT, INSERT, TRIGGER ON TABLE miovision_api.volumes_15min TO miovision_api_bot;

COMMENT ON TABLE miovision_api.volumes_15min IS E''
'NOTE: Refer instead to view volumes_15min_filtered to exclude anomalous_ranges. '
'ATR formatted Miovision data in 15 minute bins.'

COMMENT ON COLUMN miovision_api.volumes_15min.leg
IS 'leg location, e.g. E leg means both entry and exit traffic across the east side of the intersection. ';

-- Index: miovision_api.volumes_15min_classification_uid_idx
-- DROP INDEX miovision_api.volumes_15min_classification_uid_idx;
CREATE INDEX volumes_15min_classification_uid_idx
ON miovision_api.volumes_15min
USING btree(classification_uid);

-- Index: miovision_api.volumes_15min_datetime_bin_idx
-- DROP INDEX miovision_api.volumes_15min_datetime_bin_idx;
CREATE INDEX volumes_15min_datetime_bin_idx
ON miovision_api.volumes_15min
USING btree(datetime_bin);

-- Index: miovision_api.volumes_15min_intersection_uid_idx
-- DROP INDEX miovision_api.volumes_15min_intersection_uid_idx;
CREATE INDEX volumes_15min_intersection_uid_idx
ON miovision_api.volumes_15min
USING btree(intersection_uid);

-- Index: miovision_api.volumes_15min_intersection_uid_leg_dir_idx
-- DROP INDEX miovision_api.volumes_15min_intersection_uid_leg_dir_idx;
CREATE INDEX volumes_15min_intersection_uid_leg_dir_idx
ON miovision_api.volumes_15min
USING btree(
    intersection_uid,
    leg COLLATE pg_catalog."default",
    dir COLLATE pg_catalog."default"
);

-- Index: miovision_api.volumes_15min_volume_15min_uid_idx
-- DROP INDEX miovision_api.volumes_15min_volume_15min_uid_idx;
CREATE INDEX volumes_15min_volume_15min_uid_idx
ON miovision_api.volumes_15min
USING btree(volume_15min_uid ASC nulls last);

--create yearly partitions
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min', 2019::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min', 2020::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min', 2021::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min', 2022::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min', 2023::int);
