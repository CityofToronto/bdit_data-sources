DROP TABLE IF EXISTS miovision.volumes;

CREATE TABLE miovision.volumes (
	volume_uid serial,
	intersection_uid int,
	datetime_bin timestamp without time zone,
	classification_uid int,
	leg text,
	movement_uid int,
	volume int
	);
ALTER TABLE miovision.volumes
  OWNER TO aharpal;
GRANT ALL ON TABLE miovision.volumes TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE miovision.volumes TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision.volumes TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE miovision.volumes TO aharpal;