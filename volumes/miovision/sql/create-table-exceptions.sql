CREATE TABLE miovision.exceptions
(
  exceptions_uid serial NOT NULL,
  intersection_uid integer NOT NULL,
  excluded_datetime tsrange NOT NULL,
  class_type text NOT NULL,
  reason text NOT NULL,
  CONSTRAINT exceptions_uid_pkey PRIMARY KEY (exceptions_uid)
)
WITH (
  OIDS=FALSE
);