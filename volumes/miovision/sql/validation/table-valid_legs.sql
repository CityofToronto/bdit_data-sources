-- Table: miovision_validation.valid_legs

-- DROP TABLE IF EXISTS miovision_validation.valid_legs;

CREATE TABLE IF NOT EXISTS miovision_validation.valid_legs
(
    intersection_uid integer NOT NULL,
    intersection_name text COLLATE pg_catalog."default",
    start_date date NOT NULL,
    end_date date,
    classification text COLLATE pg_catalog."default" NOT NULL,
    classification_uids text COLLATE pg_catalog."default",
    leg text COLLATE pg_catalog."default" NOT NULL,
    date_processed date DEFAULT CURRENT_DATE,
    CONSTRAINT valid_legs_start_date_class_pkey PRIMARY KEY (intersection_uid, start_date, leg, classification)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_validation.valid_legs
OWNER TO miovision_validators;

REVOKE ALL ON TABLE miovision_validation.valid_legs FROM bdit_humans;
REVOKE ALL ON TABLE miovision_validation.valid_legs FROM miovision_validation_bot;

GRANT SELECT ON TABLE miovision_validation.valid_legs TO bdit_humans;

GRANT INSERT, SELECT, UPDATE ON TABLE miovision_validation.valid_legs TO miovision_validation_bot;

GRANT ALL ON TABLE miovision_validation.valid_legs TO miovision_validators;
