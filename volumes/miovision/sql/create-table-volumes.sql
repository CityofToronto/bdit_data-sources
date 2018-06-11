DROP TABLE IF EXISTS miovision.volumes;

CREATE TABLE miovision.volumes
(
  volume_uid serial NOT NULL,
  intersection_uid integer,
  datetime_bin timestamp without time zone,
  classification_uid integer,
  leg text,
  movement_uid integer,
  volume integer,
  volumes_15min_tmc_uid integer,
  CONSTRAINT volumes_volumes_15min_tmc_uid_fkey FOREIGN KEY (volumes_15min_tmc_uid)
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
GRANT INSERT, TRUNCATE ON TABLE miovision.volumes TO alouis2;
GRANT UPDATE, INSERT ON TABLE miovision.volumes TO rliu;

-- Index: miovision.volumes_intersection_uid_idx

-- DROP INDEX miovision.volumes_intersection_uid_idx;

CREATE INDEX volumes_intersection_uid_idx
  ON miovision.volumes
  USING btree
  (intersection_uid);

