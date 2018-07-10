CREATE OR REPLACE FUNCTION miovision.populate_report_dates(start_date DATE, end_date DATE)
  RETURNS integer AS
$BODY$
BEGIN

	INSERT INTO miovision.report_dates

	WITH classes(y) AS (
		SELECT UNNEST(a) FROM ( VALUES(ARRAY['Vehicles','Pedestrians','Cyclists'])) x(a)
	)

	SELECT intersection_uid, classes.y AS class_type, CASE WHEN datetime_bin <= '2017-11-11' THEN 'Baseline' ELSE to_char(date_trunc('month',datetime_bin),'Mon YYYY') END AS period_type, datetime_bin::date AS datetime_bin
	FROM miovision.volumes_15min
	INNER JOIN miovision.intersections USING (intersection_uid)
	CROSS JOIN classes
	WHERE datetime_bin::time >= '06:00' AND datetime_bin::time < '20:00' AND EXTRACT(isodow FROM datetime_bin) <= 5
	AND datetime_bin >= start_date AND datetime_bin <= end_date
	AND NOT(intersection_uid IN (1,5,10,22,26) AND 
							datetime_bin <= '2017-11-11' 
                            AND NOT (datetime_bin >= '2017-10-30' AND datetime_bin < '2017-11-04')
                            AND NOT (datetime_bin >= '2017-11-06' AND datetime_bin < '2017-11-10') AND 
							classes.y IN ('Pedestrians','Cyclists') -- Bathurst
			)
	AND NOT (intersection_uid IN (2,6,12,23,27) AND 
							datetime_bin <= '2017-11-11' 
                            AND NOT (datetime_bin >= '2017-11-08' AND datetime_bin< '2017-11-10' ) AND 
							classes.y IN ('Pedestrians','Cyclists') -- Spadina
			)
	AND NOT (intersection_uid IN (3,7,17,24,28,31) AND 
							datetime_bin <= '2017-11-11' AND
							NOT  ( datetime_bin >= '2017-11-06' AND  datetime_bin < '2017-11-10') AND 
							classes.y IN ('Pedestrians','Cyclists') -- Bay
							)
	AND NOT (intersection_uid IN (4,8,20,25,29) AND 
							datetime_bin <= '2017-11-11' AND
							NOT  ( datetime_bin >= '2017-11-01' AND datetime_bin < '2017-11-10') AND 
							classes.y IN ('Pedestrians','Cyclists') -- Jarvis
							)
	GROUP BY intersection_uid, classes.y, datetime_bin::date, period_type
	HAVING COUNT(DISTINCT datetime_bin::time) >= 40;
RETURN 1;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  SECURITY DEFINER
  COST 100;
ALTER FUNCTION miovision.refresh_views()
  OWNER TO dbadmin;
  GRANT EXECUTE ON FUNCTION miovision.refresh_views() TO bdit_humans;