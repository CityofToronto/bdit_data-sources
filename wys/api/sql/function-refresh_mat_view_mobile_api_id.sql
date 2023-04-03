CREATE OR REPLACE FUNCTION wys.refresh_mat_view_mobile_api_id()
RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE SECURITY DEFINER 
AS $BODY$
    REFRESH MATERIALIZED VIEW CONCURRENTLY wys.mobile_api_id WITH DATA ;
$BODY$;
REVOKE EXECUTE ON FUNCTION wys.refresh_mat_view_mobile_api_id()FROM public;
GRANT EXECUTE ON FUNCTION wys.refresh_mat_view_mobile_api_id()TO wys_bot;