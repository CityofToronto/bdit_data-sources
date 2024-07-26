--testing as gwolofs.volumes_15min_mvt_unfiltered

CREATE TABLE miovision_api.volumes_15min_mvt_unfiltered
(
    volume_15min_mvt_uid integer NOT NULL
    DEFAULT nextval(
        'miovision_api.volumes_15min_mvt_unfiltered_volume_15min_mvt_uid_seq'::regclass
    ),
    intersection_uid integer,
    datetime_bin timestamp without time zone,
    classification_uid integer,
    leg text,
    movement_uid integer,
    volume numeric,
    CONSTRAINT volumes_15min_mvt_int_dt_bin_class_leg_mvmt_uid_pkey
    PRIMARY KEY (
        intersection_uid, datetime_bin, classification_uid, leg, movement_uid
    )
)
PARTITION BY RANGE (datetime_bin)
WITH (
    oids = FALSE
);

ALTER TABLE miovision_api.volumes_15min_mvt_unfiltered OWNER TO miovision_admins;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.volumes_15min_mvt_unfiltered
TO bdit_humans WITH GRANT OPTION;
GRANT SELECT, INSERT, TRIGGER ON TABLE miovision_api.volumes_15min_mvt_unfiltered
TO miovision_api_bot;

COMMENT ON TABLE miovision_api.volumes_15min_mvt_unfiltered IS E''
'NOTE: Refer instead to view volumes_15min_mvt_filtered to exclude anomalous_ranges. '
'TMC formatted Miovision data in 15 minute bins. 0-padded for classifications 1,2,6,10 to '
'make averaging easier.';

-- Index: miovision_api.volumes_15min_mvt_unfiltered_classification_uid_idx
-- DROP INDEX miovision_api.volumes_15min_mvt_unfiltered_classification_uid_idx;
CREATE INDEX volumes_15min_mvt_classification_uid_idx
ON miovision_api.volumes_15min_mvt_unfiltered
USING btree (classification_uid);

-- Index: miovision_api.volumes_15min_mvt_unfiltered_datetime_bin_idx
-- DROP INDEX miovision_api.volumes_15min_mvt_unfiltered_datetime_bin_idx;
CREATE INDEX volumes_15min_mvt_datetime_bin_idx
ON miovision_api.volumes_15min_mvt_unfiltered
USING btree (datetime_bin);

-- Index: miovision_api.volumes_15min_mvt_unfiltered_intersection_uid_idx
-- DROP INDEX miovision_api.volumes_15min_mvt_unfiltered_intersection_uid_idx;
CREATE INDEX volumes_15min_mvt_intersection_uid_idx
ON miovision_api.volumes_15min_mvt_unfiltered
USING btree (intersection_uid);

-- Index: miovision_api.volumes_15min_mvt_unfiltered_leg_movement_uid_idx
-- DROP INDEX miovision_api.volumes_15min_mvt_unfiltered_leg_movement_uid_idx;
CREATE INDEX volumes_15min_mvt_leg_movement_uid_idx
ON miovision_api.volumes_15min_mvt_unfiltered
USING btree (
    leg COLLATE pg_catalog."default",
    movement_uid
);

-- Index: miovision_api.volumes_15min_mvt_unfiltered_volume_15min_mvt_uid_idx
-- DROP INDEX miovision_api.volumes_15min_mvt_unfiltered_volume_15min_mvt_uid_idx;
CREATE INDEX volumes_15min_mvt_volume_15min_mvt_uid_idx
ON miovision_api.volumes_15min_mvt_unfiltered
USING btree (volume_15min_mvt_uid ASC NULLS LAST);

--create yearly partitions
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt_unfiltered', 2019::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt_unfiltered', 2020::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt_unfiltered', 2021::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt_unfiltered', 2022::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_mvt_unfiltered', 2023::int);


CREATE TABLE gwolofs.volume_15min_mvt_unfiltered_2024 PARTITION OF gwolofs.volumes_15min_mvt_unfiltered FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
CREATE TABLE gwolofs.volume_15min_mvt_unfiltered_2023 PARTITION OF gwolofs.volumes_15min_mvt_unfiltered FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

INSERT INTO gwolofs.volumes_15min_mvt_unfiltered (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)
SELECT
    v.intersection_uid,
    datetime_bin_15(v.datetime_bin) AS datetime_bin,
    v.classification_uid,
    v.leg,
    v.movement_uid,
    SUM(volume)
FROM miovision_api.volumes_2023 AS v
GROUP BY
    v.intersection_uid,
    datetime_bin_15(v.datetime_bin),
    v.classification_uid,
    v.leg,
    v.movement_uid;

INSERT INTO gwolofs.volumes_15min_mvt_unfiltered (intersection_uid, datetime_bin, classification_uid, leg, movement_uid, volume)
SELECT
    v.intersection_uid,
    datetime_bin_15(v.datetime_bin) AS datetime_bin,
    v.classification_uid,
    v.leg,
    v.movement_uid,
    SUM(volume)
FROM miovision_api.volumes_2024 AS v
GROUP BY
    v.intersection_uid,
    datetime_bin_15(v.datetime_bin),
    v.classification_uid,
    v.leg,
    v.movement_uid;