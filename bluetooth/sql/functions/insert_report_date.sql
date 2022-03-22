-- FUNCTION: mohan.insert_report_date()

-- DROP FUNCTION mohan.insert_report_date();

CREATE OR REPLACE FUNCTION bluetooth.insert_report_date(
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
	
	begin
		with x AS 
(
SELECT analysis_id, max(datetime_bin::date) as last_reported_date
from bluetooth.aggr_5min
GROUP by analysis_id
	)
UPDATE bluetooth.routes
SET date_last_received = (SELECT last_reported_date from x where x.analysis_id = bluetooth.routes.analysis_id)	
		;
		end; 
		
$BODY$;

ALTER FUNCTION bluetooth.insert_report_date()
    OWNER TO mohan;
