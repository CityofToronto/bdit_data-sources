
CREATE OR REPLACE FUNCTION aggregate_15_min_crossover()
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
		INNER JOIN 
			(SELECT intersection_uid, classification_uid, leg_old, A.movement_uid, leg_new, dir FROM miovision.movement_map A
			JOIN miovision.intersection_movements B ON B.movement_uid=A.movement_uid AND B.leg=A.leg_old) B 
		ON B.leg_old = A.leg AND B.movement_uid = A.movement_uid AND B.intersection_uid=A.intersection_uid AND B.classification_uid=A.classification_uid
		WHERE A.processed IS NULL
		GROUP BY A.intersection_uid, A.datetime_bin, A.classification_uid, B.leg_new, B.dir
		ORDER BY A.datetime_bin, A.intersection_uid, A.classification_uid, B.leg_new, B.dir
	),
	insert_atr AS (
	INSERT INTO miovision.volumes_15min(intersection_uid, datetime_bin, classification_uid, leg, dir, volume)
	SELECT A.intersection_uid, B.datetime_bin, A.classification_uid, A.leg, A.dir, C.volume
	FROM (SELECT intersection_uid, classification_uid, leg, dir FROM transformed GROUP BY intersection_uid, classification_uid, leg, dir) AS A
	INNER JOIN (SELECT intersection_uid, datetime_bin FROM transformed GROUP BY intersection_uid, datetime_bin) AS B USING (intersection_uid)
	LEFT JOIN transformed C USING (intersection_uid, datetime_bin, classification_uid, leg, dir)
	RETURNING volume_15min_uid, intersection_uid, datetime_bin, classification_uid, leg, dir)
	
	--Updates crossover table with new IDs
	INSERT INTO atr_tmc_uid (volume_15min_tmc_uid, volume_15min_uid)
	SELECT c.volume_15min_tmc_uid, a.volume_15min_uid
	FROM miovision.volumes_15min A
	LEFT JOIN miovision.movement_map B on B.leg_new = A.leg AND B.dir = A.dir
	FULL OUTER JOIN miovision.volumes_15min_tmc C
	ON a.datetime_bin=c.datetime_bin
	AND a.intersection_uid=c.intersection_uid
	AND b.leg_old=c.leg
	AND b.movement_uid=c.movement_uid
	AND a.classification_uid=c.classification_uid
	ORDER BY volume_15min_uid;

	--Sets processed column to TRUE
	UPDATE miovision.volumes_15min_tmc a
	SET processed = TRUE
	FROM miovision.atr_tmc_uid b
	WHERE processed IS NULL
	AND a.volume_15min_tmc_uid=b.volume_15min_tmc_uid;
	
	RETURN NULL;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
