CREATE OR REPLACE VIEW miovision.report_daily AS

SELECT 	intersection_uid,
	intersection_name,
	street_main,
	street_cross,
	class_type,
	dir,
	period_type,
	A.datetime_bin::date AS dt,
	B.period_name,
	SUM(volume) AS total_volume

FROM miovision.report_volumes_15min A
CROSS JOIN miovision.periods B
INNER JOIN miovision.class_types USING (class_type_id)
INNER JOIN miovision.intersections C USING (intersection_uid)

WHERE 	A.datetime_bin::time <@ B.period_range AND
	B.period_name IN ('14 Hour','AM Peak Period','PM Peak Period') AND
	dir IN ('EB','WB') AND
	street_cross IN ('Bathurst','Spadina','Bay','Jarvis') AND
	(
		(street_cross IN ('Bathurst') AND leg IN ('E','S','N')) OR
		(street_cross IN ('Jarvis') AND leg IN ('W','S','N')) OR
		(street_cross NOT IN ('Bathurst','Jarvis') AND ( 
			(dir = 'EB' AND leg IN ('W','N','S')) OR
			(dir = 'WB' AND leg IN ('E','N','S'))
			)
		)
	) AND
	NOT (
		class_type IN ('Vehicles','Cyclists') AND 	
		(
			(dir = 'EB' AND street_main IN ('Wellington','Richmond')) OR 
			(dir = 'WB' AND street_main IN ('Adelaide'))
		)
	)
GROUP BY intersection_uid,
	intersection_name,
	street_main,
	street_cross,
	period_type,
	class_type,
	dir,
	dt,
	B.period_name
;