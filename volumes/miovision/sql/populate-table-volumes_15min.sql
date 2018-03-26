TRUNCATE miovision.volumes_15min;

WITH transformed AS (
	SELECT 	A.intersection_uid,
		A.datetime_bin,
		A.classification_uid,
		B.leg_new as leg,
		B.dir,
		SUM(A.volume) AS volume

	FROM miovision.volumes_15min_tmc A
	INNER JOIN miovision.movement_map B ON B.leg_old = A.leg AND B.movement_uid = A.movement_uid

	GROUP BY A.intersection_uid, A.datetime_bin, A.classification_uid, B.leg_new, B.dir
	ORDER BY A.datetime_bin, A.intersection_uid, A.classification_uid, B.leg_new, B.dir
)

INSERT INTO miovision.volumes_15min(intersection_uid, datetime_bin, classification_uid, leg, dir, volume)
SELECT A.intersection_uid, B.datetime_bin, A.classification_uid, A.leg, A.dir, COALESCE(C.volume,0) AS volume
FROM (SELECT intersection_uid, classification_uid, leg, dir FROM transformed GROUP BY intersection_uid, classification_uid, leg, dir) AS A
INNER JOIN (SELECT intersection_uid, datetime_bin FROM transformed GROUP BY intersection_uid, datetime_bin) AS B USING (intersection_uid)
LEFT JOIN transformed C USING (intersection_uid, datetime_bin, classification_uid, leg, dir)
ORDER BY C.intersection_uid, C.datetime_bin, C.classification_uid, C.leg, C.dir