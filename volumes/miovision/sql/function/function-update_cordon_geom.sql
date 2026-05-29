-- FUNCTION: miovision_api.update_cordon_geom()

-- DROP FUNCTION IF EXISTS miovision_api.update_cordon_geom();

CREATE OR REPLACE FUNCTION miovision_api.update_cordon_geom()
RETURNS trigger
LANGUAGE 'plpgsql'
COST 100
VOLATILE NOT LEAKPROOF
AS $BODY$
BEGIN
    NEW.geoms = (
        SELECT st_union(geom)
        FROM miovision_api.intersections
        WHERE intersection_uid = ANY(NEW.intersection_uids)
    );
RETURN NEW;
END;
$BODY$;

ALTER FUNCTION miovision_api.update_cordon_geom()
OWNER TO miovision_admins;
