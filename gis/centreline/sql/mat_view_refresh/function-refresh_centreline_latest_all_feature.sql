-- DROP FUNCTION IF EXISTS gis_core.refresh_centreline_latest_all_feature();

CREATE OR REPLACE FUNCTION gis_core.refresh_centreline_latest_all_feature()
RETURNS void
LANGUAGE sql
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
REFRESH MATERIALIZED VIEW CONCURRENTLY gis_core.centreline_latest_all_feature WITH DATA;
$BODY$;

ALTER FUNCTION gis_core.refresh_centreline_latest_all_feature()
OWNER TO gis_admins;

GRANT EXECUTE ON FUNCTION gis_core.refresh_centreline_latest_all_feature() TO gcc_bot;

GRANT EXECUTE ON FUNCTION gis_core.refresh_centreline_latest_all_feature() TO gis_admins;

REVOKE ALL ON FUNCTION gis_core.refresh_centreline_latest_all_feature() FROM public;

COMMENT ON FUNCTION gis_core.refresh_centreline_latest_all_feature()
IS 'Function to refresh gis_core.centreline_latest_all_feature, to gcc_bot permission to refresh.';
