DROP TABLE IF EXISTS miovision.volumes_15min_tmc;

CREATE TABLE miovision.volumes_15min_tmc
(
	volume_15min_tmc_uid serial NOT NULL,
	intersection_uid integer,
	datetime_bin timestamp without time zone,
	classification_uid integer,
	leg text,
	movement_uid integer,
	volume numeric
);
ALTER TABLE miovision.volumes_15min_tmc
  OWNER TO aharpal;
GRANT ALL ON TABLE miovision.volumes_15min_tmc TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE miovision.volumes_15min_tmc TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision.volumes_15min_tmc TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE miovision.volumes_15min_tmc TO aharpal;