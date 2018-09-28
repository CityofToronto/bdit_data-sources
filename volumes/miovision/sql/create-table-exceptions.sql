CREATE TABLE miovision.exceptions
(
  exceptions_uid serial NOT NULL,
  intersection_uid integer NOT NULL,
  excluded_datetime tsrange NOT NULL,
  classification_uid integer NOT NULL,
  reason text NOT NULL,
  CONSTRAINT exceptions_uid_pkey PRIMARY KEY (exceptions_uid)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE miovision.exceptions
  OWNER TO rliu;