-- Table: miovision_validation.valid_intersections

-- DROP TABLE IF EXISTS miovision_validation.valid_intersections;

CREATE TABLE IF NOT EXISTS miovision_validation.valid_intersections
(
    intersection_uid integer NOT NULL,
    intersection_name text COLLATE pg_catalog."default" NOT NULL,
    start_date date NOT NULL,
    end_date date,
    classification text COLLATE pg_catalog."default" NOT NULL,
    classification_uids text COLLATE pg_catalog."default" NOT NULL,
    processed_date date DEFAULT CURRENT_DATE,
    CONSTRAINT valid_intersections_start_date_class_pkey PRIMARY KEY (intersection_uid, start_date, classification)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_validation.valid_intersections
OWNER TO miovision_validators;

REVOKE ALL ON TABLE miovision_validation.valid_intersections FROM bdit_humans;
REVOKE ALL ON TABLE miovision_validation.valid_intersections FROM miovision_validation_bot;

GRANT SELECT ON TABLE miovision_validation.valid_intersections TO bdit_humans;

GRANT INSERT, SELECT, UPDATE ON TABLE miovision_validation.valid_intersections TO miovision_validation_bot;

GRANT ALL ON TABLE miovision_validation.valid_intersections TO miovision_validators;
