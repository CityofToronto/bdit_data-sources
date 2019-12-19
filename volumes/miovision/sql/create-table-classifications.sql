DROP TABLE IF EXISTS miovision.classifications;

CREATE TABLE miovision.classifications (
	classification_uid serial,
	classification text,
	location_only boolean, -- for peds and bikes, where movement isn't available, only which leg they were observed on
	class_type_id smallint,
    CONSTRAINT classifications_class_type_id_fkey FOREIGN KEY (class_type_id)
        REFERENCES miovision.class_types (class_type_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
	);
ALTER TABLE miovision.classifications
  OWNER TO aharpal;
GRANT ALL ON TABLE miovision.classifications TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE miovision.classifications TO dbadmin;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision.classifications TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE miovision.classifications TO aharpal;