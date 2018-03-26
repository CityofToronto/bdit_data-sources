DROP MATERIALIZED VIEW IF EXISTS miovision.report_volumes_15min CASCADE;
CREATE MATERIALIZED VIEW miovision.report_volumes_15min AS
	WITH valid_bins AS (
		SELECT	intersection_uid,
			class_type,
			(dt + B::time) AS datetime_bin,
			period_type
		FROM 	miovision.report_dates A
		CROSS JOIN generate_series('2017-01-01 06:00'::timestamp,'2017-01-01 19:45'::timestamp,'15 minutes'::interval) B
		ORDER BY intersection_uid, class_type, (dt + B::time)
	),
	int_avg AS (
		SELECT 	intersection_uid, 
			class_type, 
			dir, 
			leg, 
			period_type, 
			datetime_bin::time as time_bin, 
			AVG(total_volume) AS avg_volume
		FROM 	miovision.volumes_15min_by_class
		GROUP BY intersection_uid, class_type, period_type, dir, leg, datetime_bin::time
	)
		

	SELECT intersection_uid, period_type, datetime_bin, class_type, dir, leg, COALESCE(total_volume, avg_volume) AS volume
	FROM valid_bins A
	INNER JOIN int_avg B USING (intersection_uid, class_type, period_type)
	LEFT JOIN miovision.volumes_15min_by_class C USING (datetime_bin, intersection_uid, class_type, dir, leg, period_type)
	WHERE B.time_bin = A.datetime_bin::time
	ORDER BY intersection_uid, period_type, datetime_bin, class_type, dir, leg