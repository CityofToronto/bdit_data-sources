CREATE TABLE miovision_api.volumes_15min_tmc
(
  volume_15min_tmc_uid serial NOT NULL,
  intersection_uid integer,
  datetime_bin timestamp without time zone,
  classification_uid integer,
  leg text,
  movement_uid integer,
  volume numeric,
  processed boolean,
  CONSTRAINT volumes_15min_tmc_pkey PRIMARY KEY (volume_15min_tmc_uid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE miovision_api.volumes_15min_tmc
  OWNER TO rliu;
GRANT ALL ON TABLE miovision_api.volumes_15min_tmc TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE miovision_api.volumes_15min_tmc TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.volumes_15min_tmc TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE miovision_api.volumes_15min_tmc TO rliu;

-- Index: miovision_api.volumes_15min_tmc_classification_uid_idx

-- DROP INDEX miovision_api.volumes_15min_tmc_classification_uid_idx;

CREATE INDEX volumes_15min_tmc_classification_uid_idx
  ON miovision_api.volumes_15min_tmc
  USING btree
  (classification_uid);

-- Index: miovision_api.volumes_15min_tmc_datetime_bin_idx

-- DROP INDEX miovision_api.volumes_15min_tmc_datetime_bin_idx;

CREATE INDEX volumes_15min_tmc_datetime_bin_idx
  ON miovision_api.volumes_15min_tmc
  USING btree
  (datetime_bin);

-- Index: miovision_api.volumes_15min_tmc_intersection_uid_idx

-- DROP INDEX miovision_api.volumes_15min_tmc_intersection_uid_idx;

CREATE INDEX volumes_15min_tmc_intersection_uid_idx
  ON miovision_api.volumes_15min_tmc
  USING btree
  (intersection_uid);

-- Index: miovision_api.volumes_15min_tmc_leg_movement_uid_idx

-- DROP INDEX miovision_api.volumes_15min_tmc_leg_movement_uid_idx;

CREATE INDEX volumes_15min_tmc_leg_movement_uid_idx
  ON miovision_api.volumes_15min_tmc
  USING btree
  (leg COLLATE pg_catalog."default", movement_uid);

-- Index: miovision_api.volumes_15min_tmc_volume_15min_tmc_uid_idx

-- DROP INDEX miovision_api.volumes_15min_tmc_volume_15min_tmc_uid_idx;

CREATE INDEX volumes_15min_tmc_volume_15min_tmc_uid_idx
  ON miovision_api.volumes_15min_tmc
  USING btree
  (volume_15min_tmc_uid);


-- Trigger: volumes_delete on miovision_api.volumes_15min_tmc

-- DROP TRIGGER volumes_delete ON miovision_api.volumes_15min_tmc;

CREATE TRIGGER volumes_delete
  AFTER DELETE
  ON miovision_api.volumes_15min_tmc
  FOR EACH ROW
  EXECUTE PROCEDURE miovision_api.trgr_volumes_15min_tmc_delete();
