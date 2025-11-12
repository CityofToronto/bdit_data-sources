-- Table: miovision_validation.mio_spec_processed_counts

-- DROP TABLE IF EXISTS miovision_validation.mio_spec_processed_counts;

CREATE TABLE IF NOT EXISTS miovision_validation.mio_spec_processed_counts
(
    intersection_uid integer,
    count_id bigint,
    count_date date,
    datetime_bin timestamp without time zone,
    spec_movements text[] COLLATE pg_catalog."default",
    leg text COLLATE pg_catalog."default",
    spec_class text COLLATE pg_catalog."default",
    classification_uids integer[],
    movement_name text COLLATE pg_catalog."default",
    movement_uids integer[],
    spec_count numeric,
    miovision_api_volume numeric
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_validation.mio_spec_processed_counts OWNER TO miovision_validators;

REVOKE ALL ON TABLE miovision_validation.mio_spec_processed_counts FROM bdit_humans;

GRANT SELECT ON TABLE miovision_validation.mio_spec_processed_counts TO bdit_humans;
