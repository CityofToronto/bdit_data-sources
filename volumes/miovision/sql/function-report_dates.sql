-- Function: report_dates()

-- DROP FUNCTION report_dates();

CREATE OR REPLACE FUNCTION report_dates()
  RETURNS integer AS
$BODY$
BEGIN

TRUNCATE rliu.report_dates;

INSERT INTO rliu.report_dates

WITH classes(y) AS (
	SELECT UNNEST(a) FROM ( VALUES(ARRAY['Vehicles','Pedestrians','Cyclists'])) x(a)
)

SELECT intersection_uid, classes.y AS class_type, CASE WHEN datetime_bin <= '2017-11-11' THEN 'Baseline' ELSE to_char(date_trunc('month',datetime_bin),'Mon YYYY') END AS period_type, datetime_bin::date AS dt
FROM rliu.volumes_15min
INNER JOIN miovision.intersections USING (intersection_uid)
CROSS JOIN classes
WHERE datetime_bin::time >= '06:00' AND datetime_bin::time < '20:00' AND EXTRACT(isodow FROM datetime_bin) <= 5
GROUP BY intersection_uid, classes.y, datetime_bin::date, CASE WHEN datetime_bin <= '2017-11-11' THEN 'Baseline' ELSE to_char(date_trunc('month',datetime_bin),'Mon YYYY') END
HAVING COUNT(DISTINCT datetime_bin::time) >= 40;
						
DELETE FROM rliu.report_dates 	WHERE 	intersection_uid IN (1,5,10,22,26) AND 
						dt <= '2017-11-11' AND
						dt NOT IN ('2017-10-30','2017-10-31','2017-11-01','2017-11-02','2017-11-03','2017-11-06','2017-11-07','2017-11-08','2017-11-09') AND 
						class_type IN ('Pedestrians','Cyclists'); -- Bathurst

DELETE FROM rliu.report_dates 	WHERE 	intersection_uid IN (2,6,12,23,27) AND 
						dt <= '2017-11-11' AND
						dt NOT IN ('2017-11-08','2017-11-09') AND 
						class_type IN ('Pedestrians','Cyclists'); -- Spadina

DELETE FROM rliu.report_dates 	WHERE 	intersection_uid IN (3,7,17,24,28,31) AND 
						dt <= '2017-11-11' AND
						dt NOT IN ('2017-11-06','2017-11-07','2017-11-08','2017-11-09') AND 
						class_type IN ('Pedestrians','Cyclists'); -- Bay

DELETE FROM rliu.report_dates 	WHERE 	intersection_uid IN (4,8,20,25,29) AND 
						dt <= '2017-11-11' AND
						dt NOT IN ('2017-11-01','2017-11-02','2017-11-08','2017-11-09') AND 
class_type IN ('Pedestrians','Cyclists'); -- Jarvis

RETURN 1;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION report_dates()
  OWNER TO rliu;
GRANT EXECUTE ON FUNCTION report_dates() TO public;
GRANT EXECUTE ON FUNCTION report_dates() TO dbadmin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION report_dates() TO bdit_humans WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION report_dates() TO rliu;
