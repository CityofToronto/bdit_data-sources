CREATE MATERIALIZED VIEW miovision.volumes_15min_by_class AS
		SELECT 	intersection_uid,
			class_type,
			dir,
			leg,
			datetime_bin,
			period_type,
			SUM(volume) AS total_volume
		FROM miovision.volumes_15min A
		INNER JOIN miovision.classifications B USING (classification_uid)
		INNER JOIN miovision.report_dates C USING (class_type, intersection_uid)
		WHERE C.dt = A.datetime_bin::date
		GROUP BY intersection_uid, class_type, dir, leg, datetime_bin, period_type