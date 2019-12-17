CREATE TABLE miovision.class_types (
	class_type_id serial primary key,
	class_type text NOT NULL,
	UNIQUE(class_type)
);

GRANT SELECT, REFERENCES ON miovision.class_types TO bdit_humans WITH GRANT OPTION;