CREATE OR REPLACE FUNCTION bluetooth.insert_report_date(
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	
BEGIN

WITH routes_status AS (

    SELECT      analysis_id, max(datetime_bin)::date AS last_reported_date
    FROM        bluetooth.aggr_5min
    GROUP BY    analysis_id)

UPDATE bluetooth.routes
SET date_last_received = (SELECT last_reported_date 
                          FROM routes_status 
                          WHERE routes_status.analysis_id = bluetooth.routes.analysis_id);

END; 
		
$BODY$;

ALTER FUNCTION bluetooth.insert_report_date()
    OWNER TO bt_admins;

COMMENT ON FUNCTION bluetooth.insert_report_date() IS  '''This function updates the table bluetooth.routes with each routes last reported date.'''