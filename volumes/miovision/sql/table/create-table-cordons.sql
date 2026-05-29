-- Table: miovision_api.cordons

-- DROP TABLE IF EXISTS miovision_api.cordons;

CREATE TABLE IF NOT EXISTS miovision_api.cordons
(
    intersection_uids integer[],
    camera_group text COLLATE pg_catalog."default",
    label text COLLATE pg_catalog."default",
    exit_legs text[] COLLATE pg_catalog."default",
    geoms geography
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS miovision_api.cordons OWNER TO miovision_admins;

REVOKE ALL ON TABLE miovision_api.cordons FROM bdit_humans;

GRANT SELECT ON TABLE miovision_api.cordons TO bdit_humans;

GRANT ALL ON TABLE miovision_api.cordons TO miovision_admins;

-- Trigger: trg_cordon_geom

-- DROP TRIGGER IF EXISTS trg_cordon_geom ON miovision_api.cordons;

CREATE OR REPLACE TRIGGER trg_cordon_geom
BEFORE INSERT OR UPDATE 
ON miovision_api.cordons
FOR EACH ROW
EXECUTE FUNCTION miovision_api.update_cordon_geom();

COMMENT ON TABLE miovision_api.cordons
IS 'Groups of Miovision cameras+legs for Cordon/Area reporting. Use VIEW miovision_api.cordons_long for easier joins.';