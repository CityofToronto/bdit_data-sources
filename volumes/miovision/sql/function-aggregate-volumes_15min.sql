CREATE OR REPLACE FUNCTION miovision.aggregate_15_min()
RETURNS integer AS
$BODY$
BEGIN

	WITH transformed AS (
		SELECT 	A.intersection_uid,
			A.datetime_bin,
			A.classification_uid,
			B.leg_new as leg,
			B.dir,
			SUM(A.volume) AS volume,
			array_agg(volume_15min_tmc_uid) as uids

		FROM miovision.volumes_15min_tmc A
		INNER JOIN miovision.movement_map B ON B.leg_old = A.leg AND B.movement_uid = A.movement_uid
		WHERE volumes_15min_uid IS NULL
		GROUP BY A.intersection_uid, A.datetime_bin, A.classification_uid, B.leg_new, B.dir
		ORDER BY A.datetime_bin, A.intersection_uid, A.classification_uid, B.leg_new, B.dir
	)
	, aggregate_insert AS(
		INSERT INTO miovision.volumes_15min(intersection_uid, datetime_bin, classification_uid, leg, dir, volume)
		SELECT A.intersection_uid, B.datetime_bin, A.classification_uid, A.leg, A.dir, COALESCE(C.volume,0) AS volume
		FROM (SELECT intersection_uid, classification_uid, leg, dir FROM transformed GROUP BY intersection_uid, classification_uid, leg, dir) AS A
		INNER JOIN (SELECT intersection_uid, datetime_bin FROM transformed GROUP BY intersection_uid, datetime_bin) AS B USING (intersection_uid)
		LEFT JOIN transformed C USING (intersection_uid, datetime_bin, classification_uid, leg, dir)
		RETURNING volumes_15min_uid, intersection_uid, datetime_bin, classification_uid, leg, dir
	)
	UPDATE miovision.volumes_15min_tmc a
	SET volumes_15min_uid = b.volumes_15min_uid
	FROM (SELECT unnest(uids) AS volume_15min_tmc_uid, volumes_15min_uid 
		FROM transformed 
		INNER JOIN aggregate_insert USING (intersection_uid, datetime_bin, classification_uid, leg, dir)) b
	WHERE a.volume_15min_tmc_uid = b.volume_15min_tmc_uid;
RETURN 1;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;