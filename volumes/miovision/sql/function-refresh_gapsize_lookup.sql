CREATE OR REPLACE FUNCTION miovision_api.refresh_gapsize_lookup(
	)
    RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$

REFRESH MATERIALIZED VIEW CONCURRENTLY miovision_api.gapsize_lookup WITH DATA ;

$BODY$;