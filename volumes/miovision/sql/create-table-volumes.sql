-- Table: miovision.volumes

-- DROP TABLE miovision.volumes;

CREATE TABLE miovision.volumes
(
  volume_uid serial NOT NULL,
  intersection_uid integer,
  datetime_bin timestamp without time zone,
  classification_uid integer,
  leg text,
  movement_uid integer,
  volume integer,
  volume_15min_tmc_uid integer,
  CONSTRAINT volumes_volumes_15min_tmc_uid_fkey FOREIGN KEY (volume_15min_tmc_uid)
      REFERENCES miovision.volumes_15min_tmc (volume_15min_tmc_uid) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE SET NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE miovision.volumes
  OWNER TO aharpal;
GRANT ALL ON TABLE miovision.volumes TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE miovision.volumes TO aharpal;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision.volumes TO bdit_humans WITH GRANT OPTION;
GRANT UPDATE, INSERT, DELETE ON TABLE miovision.volumes TO rliu;
GRANT INSERT ON TABLE miovision.volumes TO alouis2;

-- Index: miovision.volumes_datetime_bin_idx

-- DROP INDEX miovision.volumes_datetime_bin_idx;

CREATE INDEX volumes_datetime_bin_idx
  ON miovision.volumes
  USING btree
  (datetime_bin);

-- Index: miovision.volumes_intersection_uid_classification_uid_leg_movement_ui_idx

-- DROP INDEX miovision.volumes_intersection_uid_classification_uid_leg_movement_ui_idx;

CREATE INDEX volumes_intersection_uid_classification_uid_leg_movement_ui_idx
  ON miovision.volumes
  USING btree
  (intersection_uid, classification_uid, leg COLLATE pg_catalog."default", movement_uid);

-- Index: miovision.volumes_intersection_uid_idx

-- DROP INDEX miovision.volumes_intersection_uid_idx;

CREATE INDEX volumes_intersection_uid_idx
  ON miovision.volumes
  USING btree
  (intersection_uid);

-- Index: miovision.volumes_volume_15min_tmc_uid_idx

-- DROP INDEX miovision.volumes_volume_15min_tmc_uid_idx;

CREATE INDEX volumes_volume_15min_tmc_uid_idx
  ON miovision.volumes
  USING btree
  (volume_15min_tmc_uid);

