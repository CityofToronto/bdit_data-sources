-- Table: miovision.volumes_15min_tmc

-- DROP TABLE miovision.volumes_15min_tmc;

CREATE TABLE miovision.volumes_15min_tmc
(
  volume_15min_tmc_uid serial NOT NULL,
  intersection_uid integer,
  datetime_bin timestamp without time zone,
  classification_uid integer,
  leg text,
  movement_uid integer,
  volume numeric,
  volume_15min_uid integer,
  CONSTRAINT volumes_15min_tmc_pkey PRIMARY KEY (volume_15min_tmc_uid),
  CONSTRAINT volumes_15min_tmc_intersection_uid_datetime_bin_classificat_key UNIQUE (intersection_uid, datetime_bin, classification_uid, leg, movement_uid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE miovision.volumes_15min_tmc
  OWNER TO aharpal;
GRANT ALL ON TABLE miovision.volumes_15min_tmc TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE miovision.volumes_15min_tmc TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision.volumes_15min_tmc TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE miovision.volumes_15min_tmc TO aharpal;
GRANT UPDATE, INSERT ON TABLE miovision.volumes_15min_tmc TO rliu;

-- Index: miovision.volumes_15min_tmc_classification_uid_idx

-- DROP INDEX miovision.volumes_15min_tmc_classification_uid_idx;

CREATE INDEX volumes_15min_tmc_classification_uid_idx
  ON miovision.volumes_15min_tmc
  USING btree
  (classification_uid);

-- Index: miovision.volumes_15min_tmc_datetime_bin_idx

-- DROP INDEX miovision.volumes_15min_tmc_datetime_bin_idx;

CREATE INDEX volumes_15min_tmc_datetime_bin_idx
  ON miovision.volumes_15min_tmc
  USING btree
  (datetime_bin);

-- Index: miovision.volumes_15min_tmc_intersection_uid_idx

-- DROP INDEX miovision.volumes_15min_tmc_intersection_uid_idx;

CREATE INDEX volumes_15min_tmc_intersection_uid_idx
  ON miovision.volumes_15min_tmc
  USING btree
  (intersection_uid);

-- Index: miovision.volumes_15min_tmc_leg_movement_uid_idx

-- DROP INDEX miovision.volumes_15min_tmc_leg_movement_uid_idx;

CREATE INDEX volumes_15min_tmc_leg_movement_uid_idx
  ON miovision.volumes_15min_tmc
  USING btree
  (leg COLLATE pg_catalog."default", movement_uid);

-- Index: miovision.volumes_15min_tmc_volume_15min_uid_idx

-- DROP INDEX miovision.volumes_15min_tmc_volume_15min_uid_idx;

CREATE INDEX volumes_15min_tmc_volume_15min_uid_idx
  ON miovision.volumes_15min_tmc
  USING btree
  (volume_15min_uid);

