CREATE TABLE miovision_api.volumes_15min_atr_unfiltered
(
    intersection_uid smallint NOT NULL,
    datetime_bin timestamp without time zone NOT NULL,
    classification_uid smallint NOT NULL,
    leg "char" NOT NULL, --noqa: RF06
    dir character(2) COLLATE pg_catalog."default" NOT NULL,
    volume smallint,
    CONSTRAINT volumes_15min_atr_unfiltered_pkey
    PRIMARY KEY (dir, leg, classification_uid, datetime_bin, intersection_uid)
)
PARTITION BY RANGE (datetime_bin)
WITH (
    oids = FALSE
);

ALTER TABLE miovision_api.volumes_15min_atr_unfiltered OWNER TO miovision_admins;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.volumes_15min_atr_unfiltered
TO bdit_humans WITH GRANT OPTION;
GRANT SELECT, INSERT, TRIGGER ON TABLE miovision_api.volumes_15min_atr_unfiltered
TO miovision_api_bot;

COMMENT ON TABLE miovision_api.volumes_15min_atr_unfiltered IS E''
'NOTE: Refer instead to function volumes_15min_atr_filtered to exclude anomalous_ranges. '
'ATR formatted Miovision data in 15 minute bins. 0-padded for classifications 1,2,6,10 to '
'make averaging easier.';

-- Index: miovision_api.volumes_15min_atr_unfiltered_classification_uid_idx
-- DROP INDEX miovision_api.volumes_15min_atr_unfiltered_classification_uid_idx;
CREATE INDEX volumes_15min_atr_classification_uid_idx
ON miovision_api.volumes_15min_atr_unfiltered
USING btree (classification_uid);

-- Index: miovision_api.volumes_15min_atr_unfiltered_datetime_bin_idx
-- DROP INDEX miovision_api.volumes_15min_atr_datetime_bin_idx;
CREATE INDEX volumes_15min_atr_datetime_bin_idx
ON miovision_api.volumes_15min_atr_unfiltered
USING brin (datetime_bin);

-- Index: miovision_api.volumes_15min_atr_unfiltered_intersection_uid_idx
-- DROP INDEX miovision_api.volumes_15min_atr_unfiltered_intersection_uid_idx;
CREATE INDEX volumes_15min_atr_intersection_uid_idx
ON miovision_api.volumes_15min_atr_unfiltered
USING btree (intersection_uid);

-- Index: miovision_api.volumes_15min_atr_unfiltered_leg_movement_uid_idx
-- DROP INDEX miovision_api.volumes_15min_atr_unfiltered_leg_movement_uid_idx;
CREATE INDEX volumes_15min_atr_leg_movement_uid_idx
ON miovision_api.volumes_15min_atr_unfiltered
USING btree (
    leg COLLATE pg_catalog."default",
    movement_uid
);

--create yearly partitions
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_atr_unfiltered', 2019::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_atr_unfiltered', 2020::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_atr_unfiltered', 2021::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_atr_unfiltered', 2022::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_atr_unfiltered', 2023::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_atr_unfiltered', 2024::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_atr_unfiltered', 2025::int);
SELECT miovision_api.create_yyyy_volumes_15min_partition('volumes_15min_atr_unfiltered', 2026::int);
