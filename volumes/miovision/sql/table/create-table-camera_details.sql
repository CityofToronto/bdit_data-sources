-- Table: miovision_api.camera_details

-- DROP TABLE IF EXISTS miovision_api.camera_details;

CREATE TABLE IF NOT EXISTS miovision_api.camera_details
(
    intersection_id text COLLATE pg_catalog."default" NOT NULL,
    camera_id text COLLATE pg_catalog."default" NOT NULL,
    camera_label text COLLATE pg_catalog."default",
    last_seen date NOT NULL,
    first_seen date DEFAULT CURRENT_DATE,
    CONSTRAINT camera_details_pkey PRIMARY KEY (intersection_id, camera_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.camera_details
OWNER TO miovision_api_bot;

REVOKE ALL ON TABLE miovision_api.camera_details FROM bdit_humans;

GRANT SELECT ON TABLE miovision_api.camera_details TO bdit_humans;

GRANT ALL ON TABLE miovision_api.camera_details TO miovision_admins;

GRANT ALL ON TABLE miovision_api.camera_details TO miovision_api_bot;

COMMENT ON TABLE miovision_api.camera_details
IS 'Miovision camera details. Updated by miovision_hardware Airflow DAG.';

COMMENT ON COLUMN miovision_api.camera_details.first_seen
IS 'First day camera was picked up by the pipeline.
Only accurate for cameras added after 2025-11-06 (see data-sources issue #1230).';
