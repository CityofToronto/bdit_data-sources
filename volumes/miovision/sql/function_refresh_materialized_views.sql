CREATE OR REPLACE FUNCTION miovision.refresh_views()
  RETURNS integer AS
$BODY$
BEGIN
	REFRESH MATERIALIZED VIEW miovision.report_dates_view WITH DATA;
	REFRESH MATERIALIZED VIEW miovision.volumes_15min_by_class WITH DATA;
	REFRESH MATERIALIZED VIEW miovision.report_volumes_15min WITH DATA;

RETURN 1;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  SECURITY DEFINER
  COST 100;
ALTER FUNCTION miovision.refresh_views()
  OWNER TO dbadmin;
  GRANT EXECUTE ON FUNCTION miovision.refresh_views() TO bdit_humans;