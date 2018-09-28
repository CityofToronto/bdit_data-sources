CREATE MATERIALIZED VIEW miovision.report_dates_view AS 

WITH classes(y) AS (
	SELECT UNNEST(a) FROM ( VALUES(ARRAY['Vehicles','Pedestrians','Cyclists'])) x(a)
)

SELECT intersection_uid, classes.y AS class_type,
 CASE WHEN datetime_bin <= '2017-11-11' THEN 'Baseline' 
 	  ELSE to_char(date_trunc('month',datetime_bin),'Mon YYYY') END AS period_type,
 datetime_bin::date AS dt
FROM miovision.volumes_15min
INNER JOIN miovision.intersections USING (intersection_uid)
CROSS JOIN classes
LEFT OUTER JOIN miovision_new.exceptions USING (intersection_uid, classification_uid)
WHERE datetime_bin::time >= '06:00' AND datetime_bin::time < '20:00' AND EXTRACT(isodow FROM datetime_bin) <= 5
    AND exceptions_uid IS NULL --exclude excepted data
GROUP BY intersection_uid, classes.y, dt, period_type
HAVING COUNT(DISTINCT datetime_bin::time) >= 40;
						
