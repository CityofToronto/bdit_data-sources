CREATE MATERIALIZED VIEW miovision.report_dates_view AS 

SELECT intersection_uid, classe_type_id,
 CASE WHEN datetime_bin <= '2017-11-11' THEN 'Baseline' 
 	  ELSE to_char(date_trunc('month',datetime_bin),'Mon YYYY') END AS period_type,
 datetime_bin::date AS dt
FROM miovision.volumes_15min
INNER JOIN miovision.intersections USING (intersection_uid)
CROSS JOIN (SELECT class_type_id FROM miovision.class_types) classes

WHERE datetime_bin::time >= '06:00' AND datetime_bin::time < '20:00' AND EXTRACT(isodow FROM datetime_bin) <= 5

GROUP BY intersection_uid, class_type_id, dt, period_type
HAVING COUNT(DISTINCT datetime_bin::time) >= 40;
						
