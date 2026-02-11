-- DROP FUNCTION IF EXISTS gis_core.refresh_intersection_latest();

CREATE OR REPLACE FUNCTION gis_core.refresh_intersection_latest()
RETURNS void
LANGUAGE sql
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
REFRESH MATERIALIZED VIEW CONCURRENTLY gis_core.intersection_latest WITH DATA;
$BODY$;

ALTER FUNCTION gis_core.refresh_intersection_latest()
OWNER TO gis_admins;

GRANT EXECUTE ON FUNCTION gis_core.refresh_intersection_latest() TO gcc_bot;

GRANT EXECUTE ON FUNCTION gis_core.refresh_intersection_latest() TO gis_admins;

REVOKE ALL ON FUNCTION gis_core.refresh_intersection_latest() FROM public;

COMMENT ON FUNCTION gis_core.refresh_intersection_latest()
IS 'Function to refresh gis_core.intersection_latest, to gcc_bot permission to refresh.';
