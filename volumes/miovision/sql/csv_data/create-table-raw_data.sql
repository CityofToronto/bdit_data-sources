DROP TABLE IF EXISTS miovision.raw_data;

CREATE TABLE miovision.raw_data (
	study_id bigint,
	study_name text,
	lat decimal,
	lng decimal,
	datetime_bin timestamp without time zone,
	classification text,
	entry_dir_name text,
	entry_name text,
	exit_dir_name text,
	exit_name text,
	movement text,
	volume int
	);
ALTER TABLE miovision.raw_data
  OWNER TO aharpal;
GRANT ALL ON TABLE miovision.raw_data TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE miovision.raw_data TO dbadmin;
GRANT ALL ON TABLE miovision.raw_data TO aharpal;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision.raw_data TO bdit_humans WITH GRANT OPTION;
