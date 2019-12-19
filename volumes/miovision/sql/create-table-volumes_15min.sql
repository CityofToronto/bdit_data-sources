-- Table: miovision_api.volumes_15min

-- DROP TABLE miovision_api.volumes_15min;
CREATE TABLE miovision_api.volumes_15min
(
  volume_15min_uid serial NOT NULL,
  intersection_uid integer,
  datetime_bin timestamp without time zone,
  classification_uid integer,
  leg text,
  dir text,
  volume numeric,
  CONSTRAINT volumes_15min_pkey PRIMARY KEY (volume_15min_uid),
  CONSTRAINT volumes_15min_intersection_uid_datetime_bin_classification__key UNIQUE (intersection_uid, datetime_bin, classification_uid, leg, dir)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE miovision_api.volumes_15min
  OWNER TO miovision_admins;
GRANT ALL ON TABLE miovision_api.volumes_15min TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE miovision_api.volumes_15min TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.volumes_15min TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE miovision_api.volumes_15min TO rliu;

-- Index: miovision_api.volumes_15min_classification_uid_idx

-- DROP INDEX miovision_api.volumes_15min_classification_uid_idx;

CREATE INDEX volumes_15min_classification_uid_idx
  ON miovision_api.volumes_15min
  USING btree
  (classification_uid);

-- Index: miovision_api.volumes_15min_datetime_bin_idx

-- DROP INDEX miovision_api.volumes_15min_datetime_bin_idx;

CREATE INDEX volumes_15min_datetime_bin_idx
  ON miovision_api.volumes_15min
  USING btree
  (datetime_bin);

-- Index: miovision_api.volumes_15min_intersection_uid_idx

-- DROP INDEX miovision_api.volumes_15min_intersection_uid_idx;

CREATE INDEX volumes_15min_intersection_uid_idx
  ON miovision_api.volumes_15min
  USING btree
  (intersection_uid);

-- Index: miovision_api.volumes_15min_intersection_uid_leg_dir_idx

-- DROP INDEX miovision_api.volumes_15min_intersection_uid_leg_dir_idx;

CREATE INDEX volumes_15min_intersection_uid_leg_dir_idx
  ON miovision_api.volumes_15min
  USING btree
  (intersection_uid, leg COLLATE pg_catalog."default", dir COLLATE pg_catalog."default");

-- Index: miovision_api.volumes_15min_volume_15min_uid_idx

-- DROP INDEX miovision_api.volumes_15min_volume_15min_uid_idx;

CREATE INDEX volumes_15min_volume_15min_uid_idx
  ON miovision_api.volumes_15min
  USING btree
  (volume_15min_uid);
