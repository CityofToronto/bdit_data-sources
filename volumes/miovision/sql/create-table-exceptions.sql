CREATE TABLE miovision.exceptions
(
  exceptions_uid serial NOT NULL,
  intersection_uid integer NOT NULL,
  excluded_datetime tsrange NOT NULL,
  class_type_id integer NOT NULL,
  reason text NOT NULL,
  CONSTRAINT exceptions_uid_pkey PRIMARY KEY (exceptions_uid),
  FOREIGN KEY (class_type_id)
        REFERENCES miovision_new.class_types (class_type_id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);