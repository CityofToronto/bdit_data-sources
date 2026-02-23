-- DROP FUNCTION IF EXISTS gis_core.refresh_centreline_leg_directions();

CREATE OR REPLACE FUNCTION gis_core.refresh_centreline_leg_directions()
RETURNS void
LANGUAGE sql
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
REFRESH MATERIALIZED VIEW CONCURRENTLY gis_core.centreline_leg_directions WITH DATA;
$BODY$;

ALTER FUNCTION gis_core.refresh_centreline_leg_directions()
OWNER TO gis_admins;

GRANT EXECUTE ON FUNCTION gis_core.refresh_centreline_leg_directions() TO gcc_bot;

GRANT EXECUTE ON FUNCTION gis_core.refresh_centreline_leg_directions() TO gis_admins;

REVOKE ALL ON FUNCTION gis_core.refresh_centreline_leg_directions() FROM public;

COMMENT ON FUNCTION gis_core.refresh_centreline_leg_directions()
IS 'Function to refresh gis_core.centreline_leg_directions, to gcc_bot permission to refresh.';
