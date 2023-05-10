CREATE OR REPLACE FUNCTION wys.refresh_mat_view_stationary_signs()
RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$
    REFRESH MATERIALIZED VIEW CONCURRENTLY wys.stationary_signs WITH DATA ;
$BODY$;
REVOKE EXECUTE ON FUNCTION wys.refresh_mat_view_stationary_signs()FROM public;
GRANT EXECUTE ON FUNCTION wys.refresh_mat_view_stationary_signs()TO wys_bot;