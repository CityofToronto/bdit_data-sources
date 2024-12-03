-- Table: miovision_api.camera_details

-- DROP TABLE IF EXISTS miovision_api.camera_details;

CREATE TABLE IF NOT EXISTS miovision_api.camera_details
(
    intersection_id text COLLATE pg_catalog."default",
    intersection_lat numeric,
    intersection_long numeric,
    intersection_name text COLLATE pg_catalog."default",
    intersection_customid text COLLATE pg_catalog."default",
    camera_id text COLLATE pg_catalog."default",
    camera_type text COLLATE pg_catalog."default",
    camera_label text COLLATE pg_catalog."default",
    camera_streamurl text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.camera_details OWNER TO
miovision_api_bot;

REVOKE ALL ON TABLE miovision_api.camera_details FROM bdit_humans;
GRANT SELECT ON TABLE miovision_api.camera_details TO bdit_humans;

GRANT ALL ON TABLE miovision_api.camera_details TO miovision_admins;
GRANT ALL ON TABLE miovision_api.camera_details TO miovision_api_bot;

COMMENT ON TABLE miovision_api.camera_details IS
'Miovision camera details, excluding decommissioned intersections. '
'Update using `get_camera_info.py` script.';