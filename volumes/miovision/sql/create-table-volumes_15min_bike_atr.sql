CREATE TABLE miovision_api.volumes_15min_bike_atr
(
  volume_15min_tmc_uid integer NOT NULL DEFAULT nextval('miovision_api.volumes_15min_tmc_volume_15min_tmc_uid_seq'::regclass),
  intersection_uid integer,
  datetime_bin timestamp without time zone,
  classification_uid integer,
  leg text,
  movement_uid integer,
  volume numeric,
  processed boolean,
  CONSTRAINT volumes_15min_bike_atr_pkey PRIMARY KEY (volume_15min_tmc_uid),
  CONSTRAINT volumes_15min_bike_atr_intersection_uid_datetime_bin_classificat_key UNIQUE (intersection_uid, datetime_bin, classification_uid, leg, movement_uid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE miovision_api.volumes_15min_bike_atr
  OWNER TO miovision_admins;
GRANT ALL ON TABLE miovision_api.volumes_15min_bike_atr TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE miovision_api.volumes_15min_bike_atr TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.volumes_15min_bike_atr TO bdit_humans WITH GRANT OPTION;

-- Index: miovision_api.volumes_15min_bike_atr_classification_uid_idx

-- DROP INDEX miovision_api.volumes_15min_bike_atr_classification_uid_idx;

CREATE INDEX volumes_15min_bike_atr_classification_uid_idx
  ON miovision_api.volumes_15min_bike_atr
  USING btree
  (classification_uid);

-- Index: miovision_api.volumes_15min_bike_atr_datetime_bin_idx

-- DROP INDEX miovision_api.volumes_15min_bike_atr_datetime_bin_idx;

CREATE INDEX volumes_15min_bike_atr_datetime_bin_idx
  ON miovision_api.volumes_15min_bike_atr
  USING btree
  (datetime_bin);

-- Index: miovision_api.volumes_15min_bike_atr_intersection_uid_idx

-- DROP INDEX miovision_api.volumes_15min_bike_atr_intersection_uid_idx;

CREATE INDEX volumes_15min_bike_atr_intersection_uid_idx
  ON miovision_api.volumes_15min_bike_atr
  USING btree
  (intersection_uid);

-- Index: miovision_api.volumes_15min_bike_atr_leg_movement_uid_idx

-- DROP INDEX miovision_api.volumes_15min_bike_atr_leg_movement_uid_idx;

CREATE INDEX volumes_15min_bike_atr_leg_movement_uid_idx
  ON miovision_api.volumes_15min_bike_atr
  USING btree
  (leg COLLATE pg_catalog."default", movement_uid);

-- Index: miovision_api.volumes_15min_bike_atr_volume_15min_tmc_uid_idx

-- DROP INDEX miovision_api.volumes_15min_bike_atr_volume_15min_tmc_uid_idx;

CREATE INDEX volumes_15min_bike_atr_volume_15min_tmc_uid_idx
  ON miovision_api.volumes_15min_bike_atr
  USING btree
  (volume_15min_tmc_uid);

