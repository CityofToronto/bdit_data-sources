DROP TABLE IF EXISTS miovision.classifications;

CREATE TABLE miovision.classifications (
	classification_uid serial,
	classification text,
	location_only boolean -- for peds and bikes, where movement isn't available, only which leg they were observed on
	);
ALTER TABLE miovision.classifications
  OWNER TO aharpal;
GRANT ALL ON TABLE miovision.classifications TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE miovision.classifications TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision.classifications TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE miovision.classifications TO aharpal;