-- DROP FUNCTION IF EXISTS traffic.refresh_svc_centreline_directions();

CREATE OR REPLACE FUNCTION traffic.refresh_svc_centreline_directions()
RETURNS void
LANGUAGE sql
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
REFRESH MATERIALIZED VIEW CONCURRENTLY traffic.svc_centreline_directions WITH DATA;
$BODY$;

ALTER FUNCTION traffic.refresh_svc_centreline_directions()
OWNER TO gis_admins;

GRANT EXECUTE ON FUNCTION traffic.refresh_svc_centreline_directions() TO gcc_bot;

GRANT EXECUTE ON FUNCTION traffic.refresh_svc_centreline_directions() TO traffic_admins;

REVOKE ALL ON FUNCTION traffic.refresh_svc_centreline_directions() FROM public;

COMMENT ON FUNCTION traffic.refresh_svc_centreline_directions()
IS 'Function to refresh traffic.svc_centreline_directions, to gcc_bot permission to refresh.';
