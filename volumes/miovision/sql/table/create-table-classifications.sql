CREATE TABLE miovision_api.classifications
(
    classification_uid integer DEFAULT nextval(
        'miovision_api.classifications_classification_uid_seq'::regclass
    ) PRIMARY KEY,
    classification text COLLATE pg_catalog."default",
    -- for peds and bikes, where movement isn't available, only which leg they were observed on
    location_only boolean,
    class_type text COLLATE pg_catalog."default",
    zero_padded boolean --identify vehicle classifications which will have value 0 at all up-times
)
WITH (
    oids = FALSE
)
TABLESPACE pg_default;
ALTER TABLE miovision_api.classifications OWNER TO miovision_admins;

---add zero_padded by classification_uid
UPDATE miovision_api.classifications
SET zero_padded = coalesce(classification_uid IN (1, 2, 6, 10), FALSE)