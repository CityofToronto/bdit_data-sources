CREATE MATERIALIZED VIEW miovision.volumes_15min_by_class AS
		SELECT 	a.intersection_uid,
			b.class_type_id,
			dir,
			leg,
			datetime_bin,
			period_type,
			SUM(volume) AS total_volume
		FROM miovision.volumes_15min A
		INNER JOIN miovision.classifications B USING (classification_uid)
		INNER JOIN miovision.report_dates_view C USING (class_type_id, intersection_uid)
		LEFT OUTER JOIN miovision.exceptions e ON (a.intersection_uid, b.class_type_id) = (e.intersection_uid, e.class_type_id) AND datetime_bin <@ e.excluded_datetime
 		WHERE C.dt <= A.datetime_bin AND C.dt + interval '1 day'> A.datetime_bin
    AND exceptions_uid IS NULL --exclude excepted data
		GROUP BY a.intersection_uid, b.class_type_id, dir, leg, datetime_bin, period_type;


CREATE INDEX ON miovision.volumes_15min_by_class (datetime_bin, intersection_uid, class_type_id, dir, leg, period_type);