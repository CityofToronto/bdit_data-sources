CREATE OR REPLACE FUNCTION vds.add_entity_locations_geoms()
    RETURNS trigger
    LANGUAGE plpgsql
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
BEGIN

    -- Set geom on the incoming row directly
    NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);

    RETURN NEW;

END;
$BODY$;

ALTER FUNCTION vds.add_entity_locations_geoms()
OWNER TO vds_admins;

COMMENT ON FUNCTION vds.add_entity_locations_geoms()
IS 'After insert into vds.entity_locations, add missing geoms from lat/lon.';

CREATE OR REPLACE TRIGGER add_entity_locations_geoms_vdsdata
AFTER INSERT OR UPDATE
ON vds.entity_locations
FOR EACH ROW
EXECUTE FUNCTION vds.add_entity_locations_geoms();
