CREATE MATERIALIZED VIEW miovision.volumes_15min_new AS

WITH vol_15min_tmc AS (
	WITH vol_1min AS (
		SELECT B.intersection_uid, (A.datetime_bin AT TIME ZONE 'America/Toronto') AS datetime_bin, C.classification_uid, A.entry_dir_name as leg, D.movement_uid, A.volume
		FROM miovision.raw_data_new A
		INNER JOIN miovision.intersections B ON regexp_replace(A.study_name,'Yong\M','Yonge') = B.intersection_name
		INNER JOIN miovision.movements D USING (movement)
		INNER JOIN miovision.classifications C USING (classification, location_only)
		ORDER BY (A.datetime_bin AT TIME ZONE 'America/Toronto'), B.intersection_uid, C.classification_uid, A.entry_name, D.movement_uid
	)

	SELECT 	A.intersection_uid,
		TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900) AS datetime_bin,
		A.classification_uid,
		A.leg,
		A.movement_uid,
		SUM(A.volume) AS volume
	FROM vol_1min A
	GROUP BY A.intersection_uid, TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900), A.classification_uid, A.leg, A.movement_uid
	ORDER BY TIMESTAMP WITHOUT TIME ZONE 'epoch' + INTERVAL '1 second' * (floor((extract('epoch' from A.datetime_bin)) / 900) * 900), A.intersection_uid, A.classification_uid, A.leg, A.movement_uid
),

transformed AS (
	SELECT 	A.intersection_uid,
		A.datetime_bin,
		A.classification_uid,
		B.leg_new as leg,
		B.dir,
		SUM(A.volume) AS volume

	FROM vol_15min_tmc A
	INNER JOIN miovision.movement_map B ON B.leg_old = A.leg AND B.movement_uid = A.movement_uid

	GROUP BY A.intersection_uid, A.datetime_bin, A.classification_uid, B.leg_new, B.dir
	ORDER BY A.datetime_bin, A.intersection_uid, A.classification_uid, B.leg_new, B.dir
)

SELECT A.intersection_uid, B.datetime_bin, A.classification_uid, A.leg, A.dir, COALESCE(C.volume,0) AS volume
FROM (SELECT intersection_uid, classification_uid, leg, dir FROM transformed GROUP BY intersection_uid, classification_uid, leg, dir) AS A
INNER JOIN (SELECT intersection_uid, datetime_bin FROM transformed GROUP BY intersection_uid, datetime_bin) AS B USING (intersection_uid)
LEFT JOIN transformed C USING (intersection_uid, datetime_bin, classification_uid, leg, dir)
ORDER BY C.intersection_uid, C.datetime_bin, C.classification_uid, C.leg, C.dir