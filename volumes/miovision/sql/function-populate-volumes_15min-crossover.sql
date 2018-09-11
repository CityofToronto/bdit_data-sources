CREATE OR REPLACE FUNCTION aggregate_15_min_crossover()
  RETURNS integer AS
$BODY$
BEGIN

    
    WITH transformed AS (
        SELECT     A.intersection_uid,
            A.datetime_bin,
            A.classification_uid,
            B.leg_new as leg,
            B.dir,
            SUM(A.volume) AS volume,
            array_agg(volume_15min_tmc_uid) as uids

        FROM volumes_15min_tmc A
        INNER JOIN 
            (SELECT intersection_uid, classification_uid, leg_old, A.movement_uid, leg_new, dir FROM miovision.movement_map A
            JOIN miovision.intersection_movements B ON B.movement_uid=A.movement_uid AND B.leg=A.leg_old) B 
        ON B.leg_old = A.leg AND B.movement_uid = A.movement_uid AND B.intersection_uid=A.intersection_uid AND B.classification_uid=A.classification_uid
        WHERE A.processed IS NULL
        GROUP BY A.intersection_uid, A.datetime_bin, A.classification_uid, B.leg_new, B.dir
    ),
    insert_atr AS (
    INSERT INTO miovision.volumes_15min(intersection_uid, datetime_bin, classification_uid, leg, dir, volume)
    SELECT intersection_uid, datetime_bin, classification_uid, leg, dir, volume
    FROM transformed
    RETURNING volume_15min_uid, intersection_uid, datetime_bin, classification_uid, leg, dir)
    
    --Updates crossover table with new IDs
    , insert_crossover AS(
    INSERT INTO atr_tmc_uid (volume_15min_tmc_uid, volume_15min_uid)
    SELECT volume_15min_tmc_uid, volume_15min_uid
    FROM rliu.volumes_15min A
    INNER JOIN (SELECT intersection_uid, datetime_bin, classification_uid, leg, dir, unnest(uids) as volume_15min_tmc_uid FROM transformed) B
        ON A.datetime_bin=B.datetime_bin
        AND A.intersection_uid=B.intersection_uid
        AND A.leg=B.leg
        AND A.classification_uid=B.classification_uid
    ORDER BY volume_15min_uid

    )
    --Sets processed column to TRUE
    UPDATE volumes_15min_tmc a
    SET processed = TRUE
    FROM (SELECT unnest(uids) as volume_15min_tmc_uid FROM transformed) b 
    WHERE a.volume_15min_tmc_uid=b.volume_15min_tmc_uid;
    
    RETURN NULL;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
