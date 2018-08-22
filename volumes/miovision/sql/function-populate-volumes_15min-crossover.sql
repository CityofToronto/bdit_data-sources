
-- Function: aggregate_15_min()

-- DROP FUNCTION aggregate_15_min();

CREATE OR REPLACE FUNCTION aggregate_15_min()
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

		FROM rliu.volumes_15min_tmc A
		INNER JOIN miovision.movement_map B ON B.leg_old = A.leg AND B.movement_uid = A.movement_uid
		WHERE A.processed IS NULL
		GROUP BY A.intersection_uid, A.datetime_bin, A.classification_uid, B.leg_new, B.dir
		ORDER BY A.datetime_bin, A.intersection_uid, A.classification_uid, B.leg_new, B.dir
	)

	INSERT INTO rliu.volumes_15min(intersection_uid, datetime_bin, classification_uid, leg, dir, volume)
	SELECT A.intersection_uid, B.datetime_bin, A.classification_uid, A.leg, A.dir, COALESCE(C.volume,0) AS volume
	FROM (SELECT intersection_uid, classification_uid, leg, dir FROM transformed GROUP BY intersection_uid, classification_uid, leg, dir) AS A
	INNER JOIN (SELECT intersection_uid, datetime_bin FROM transformed GROUP BY intersection_uid, datetime_bin) AS B USING (intersection_uid)
	LEFT JOIN transformed C USING (intersection_uid, datetime_bin, classification_uid, leg, dir);
		
	--Updates crossover table with new IDs
	INSERT INTO atr_tmc_uid (volume_15min_tmc_uid, volume_15min_uid)
	SELECT c.volume_15min_tmc_uid, a.volume_15min_uid
	FROM rliu.volumes_15min A
	LEFT JOIN miovision.movement_map B on B.leg_new = A.leg AND B.dir = A.dir
	FULL OUTER JOIN rliu.volumes_15min_tmc C
	ON a.datetime_bin=c.datetime_bin
	AND a.intersection_uid=c.intersection_uid
	AND b.leg_old=c.leg
	AND b.movement_uid=c.movement_uid
	AND a.classification_uid=c.classification_uid
	WHERE A.processed IS NULL
	ORDER BY volume_15min_uid;

	--Sets processed column to TRUE
	UPDATE rliu.volumes_15min_tmc a
	SET processed = TRUE
	FROM rliu.atr_tmc_uid b
	WHERE processed IS NULL
	AND a.volume_15min_tmc_uid=b.volume_15min_tmc_uid;

	UPDATE rliu.volumes_15min a
	SET processed = TRUE
	FROM rliu.atr_tmc_uid b
	WHERE processed IS NULL
	AND a.volume_15min_uid=b.volume_15min_uid;


	--Sets remaning records to FALSE if the record cannot be processed
	UPDATE rliu.volumes_15min a
	SET processed = FALSE
	WHERE processed IS NULL;
	
	UPDATE rliu.volumes_15min_tmc a
	SET processed = FALSE
	WHERE processed IS NULL;
RETURN 1;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION aggregate_15_min()
  OWNER TO rliu;
GRANT EXECUTE ON FUNCTION aggregate_15_min() TO public;
GRANT EXECUTE ON FUNCTION aggregate_15_min() TO dbadmin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION aggregate_15_min() TO bdit_humans WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION aggregate_15_min() TO rliu;
