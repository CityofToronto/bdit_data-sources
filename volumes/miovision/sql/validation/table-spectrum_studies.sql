-- Table: miovision_validation.spectrum_studies

-- DROP TABLE IF EXISTS miovision_validation.spectrum_studies;

CREATE TABLE IF NOT EXISTS miovision_validation.spectrum_studies
(
    intersection_uid integer NOT NULL,
    int_id integer,
    count_date date NOT NULL,
    count_id bigint NOT NULL,
    processed boolean DEFAULT false,
    count_veh_n_appr numeric,
    count_veh_s_appr numeric,
    count_veh_e_appr numeric,
    count_veh_w_appr numeric,
    CONSTRAINT miovision_validation_spectrum_studies_pkey PRIMARY KEY (intersection_uid, count_date, count_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_validation.spectrum_studies OWNER TO miovision_validators;

REVOKE ALL ON TABLE miovision_validation.spectrum_studies FROM bdit_humans;

GRANT SELECT ON TABLE miovision_validation.spectrum_studies TO bdit_humans;

COMMENT ON TABLE miovision_validation.spectrum_studies
IS 'Spectrum studies at Miovision intersections.
Updated daily by `miovision_validation` Airflow DAG.';
