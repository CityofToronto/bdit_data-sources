-- FUNCTION: gis.refresh_mat_view_intersection_version_date()

-- DROP FUNCTION IF EXISTS gis.refresh_mat_view_intersection_version_date();

CREATE OR REPLACE FUNCTION gis.refresh_mat_view_intersection_version_date()
RETURNS void
LANGUAGE 'sql'
COST 100
VOLATILE SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$
    REFRESH MATERIALIZED VIEW CONCURRENTLY gis.intersection_version_date WITH DATA ;
$BODY$;

COMMENT ON FUNCTION gis.refresh_mat_view_intersection_version_date()
IS 'Refresh the materialized view that lists all dates of different intersection versions';

ALTER FUNCTION gis.refresh_mat_view_intersection_version_date()
OWNER TO gis_admins;

REVOKE ALL ON FUNCTION gis.refresh_mat_view_intersection_version_date() FROM public;

GRANT EXECUTE ON FUNCTION gis.refresh_mat_view_intersection_version_date() TO gis_admins;

GRANT EXECUTE ON FUNCTION gis.refresh_mat_view_intersection_version_date() TO gcc_bot;

GRANT EXECUTE ON FUNCTION gis.refresh_mat_view_intersection_version_date() TO ptc_humans;