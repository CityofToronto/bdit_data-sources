CREATE VIEW bluetooth.routes_not_reporting_yesterday AS (
SELECT analysis_id
FROM bluetooth.all_analyses
WHERE pull_data

EXCEPT

SELECT DISTINCT analysis_id
FROM bluetooth.observations
WHERE measured_timestamp > current_date - INTERVAL '1 Day' 
);

GRANT SELECT ON bluetooth.routes_not_reporting_yesterday TO bdit_humans;
GRANT SELECT ON bluetooth.routes_not_reporting_yesterday TO bt_insert_bot;
ALTER TABLE bluetooth.routes_not_reporting_yesterday OWNER TO bt_admins;

DROP TABLE IF EXISTS bluetooth.dates_without_data;
CREATE TABLE bluetooth.dates_without_data(
	analysis_id BIGINT NOT NULL,
	day_without_data DATE NOT NULL,
	UNIQUE(analysis_id, day_without_data)
);
ALTER TABLE bluetooth.dates_without_data OWNER TO bt_admins;
GRANT SELECT ON bluetooth.dates_without_data TO bdit_humans;
GRANT SELECT, INSERT ON bluetooth.dates_without_data TO bt_insert_bot;


CREATE FUNCTION bluetooth.insert_dates_without_data() 
 RETURNS integer AS
$BODY$
BEGIN
	
	INSERT INTO bluetooth.dates_without_data
	SELECT analysis_id, current_date - 1 
	FROM bluetooth.routes_not_reporting_yesterday
	ON CONFLICT DO NOTHING;
	RETURN 1;
END
$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;
 ALTER FUNCTION bluetooth.insert_dates_without_data()
  OWNER TO bt_admins;
GRANT EXECUTE ON FUNCTION bluetooth.insert_dates_without_data() TO bt_admins;
GRANT EXECUTE ON FUNCTION bluetooth.insert_dates_without_data() TO aharpal;
GRANT EXECUTE ON FUNCTION bluetooth.insert_dates_without_data() TO bt_insert_bot;
GRANT EXECUTE ON FUNCTION bluetooth.insert_dates_without_data() TO rdumas;

	