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
  CONSTRAINT volumes_volumes_15min_mvt_uid_fkey FOREIGN KEY (volume_15min_mvt_uid)
      REFERENCES miovision_api.volumes_15min_mvt (volume_15min_mvt_uid) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE SET NULL
)
WITH (
  OIDS=FALSE
);
ALTER TABLE miovision_api.volumes
  OWNER TO miovision_admins;
GRANT ALL ON TABLE miovision_api.volumes TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE miovision_api.volumes TO aharpal;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.volumes TO bdit_humans WITH GRANT OPTION;

-- Index: miovision_api.volumes_datetime_bin_idx

-- DROP INDEX miovision_api.volumes_datetime_bin_idx;

CREATE INDEX volumes_datetime_bin_idx
  ON miovision_api.volumes
  USING brin
  (datetime_bin);

-- Index: miovision_api.volumes_intersection_uid_classification_uid_leg_movement_ui_idx

-- DROP INDEX miovision_api.volumes_intersection_uid_classification_uid_leg_movement_ui_idx;

CREATE INDEX volumes_intersection_uid_classification_uid_leg_movement_ui_idx
  ON miovision_api.volumes
  USING btree
  (intersection_uid, classification_uid, leg COLLATE pg_catalog."default", movement_uid);

-- Index: miovision_api.volumes_intersection_uid_idx

-- DROP INDEX miovision_api.volumes_intersection_uid_idx;

CREATE INDEX volumes_intersection_uid_idx
  ON miovision_api.volumes
  USING btree
  (intersection_uid);

-- Index: miovision_api.volumes_volume_15min_mvt_uid_idx

-- DROP INDEX miovision_api.volumes_volume_15min_mvt_uid_idx;

CREATE INDEX volumes_volume_15min_mvt_uid_idx
  ON miovision_api.volumes
  USING btree
  (volume_15min_mvt_uid);