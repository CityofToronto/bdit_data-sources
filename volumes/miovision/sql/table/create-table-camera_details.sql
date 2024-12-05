-- Table: miovision_api.camera_details

-- DROP TABLE IF EXISTS miovision_api.camera_details;

CREATE TABLE IF NOT EXISTS miovision_api.camera_details
(
    intersection_id text COLLATE pg_catalog."default" NOT NULL,
    api_name text COLLATE pg_catalog."default",
    camera_id text COLLATE pg_catalog."default" NOT NULL,
    camera_label text COLLATE pg_catalog."default",
    last_seen date NOT NULL,
    CONSTRAINT camera_details_pkey PRIMARY KEY (intersection_id, camera_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.camera_details
    OWNER to miovision_api_bot;

REVOKE ALL ON TABLE miovision_api.camera_details FROM bdit_humans;

GRANT SELECT ON TABLE miovision_api.camera_details TO bdit_humans;

GRANT ALL ON TABLE miovision_api.camera_details TO miovision_admins;

GRANT ALL ON TABLE miovision_api.camera_details TO miovision_api_bot;

COMMENT ON TABLE miovision_api.camera_details
IS 'Miovision camera details. Updated by miovision_hardware Airflow DAG.';