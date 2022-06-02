CREATE TABLE miovision_api.classifications
(
    classification_uid integer NOT NULL DEFAULT nextval('miovision_api.classifications_classification_uid_seq'::regclass),
    classification text COLLATE pg_catalog."default",
    location_only boolean, -- for peds and bikes, where movement isn't available, only which leg they were observed on
    class_type text COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;