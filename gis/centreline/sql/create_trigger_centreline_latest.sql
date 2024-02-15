CREATE OR REPLACE FUNCTION gis_core.centreline_latest_trigger()
    RETURNS trigger 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE NOT LEAKPROOF SECURITY DEFINER
AS $BODY$
BEGIN

REFRESH MATERIALIZED VIEW gis_core.centreline_latest;
RETURN NULL;

END;
$BODY$;

ALTER FUNCTION gis_core.centreline_latest_trigger() OWNER TO gis_admins;

COMMENT ON FUNCTION gis_core.centreline_latest_trigger() IS 'Trigger fuction that refreshes the centreline_latest mat view after an update.';

