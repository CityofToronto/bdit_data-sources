CREATE OR REPLACE FUNCTION miovision_api.aggregate_15_min(
    start_date date,
    end_date date)
  RETURNS integer AS
$BODY$
BEGIN
--Creates the ATR bins
    WITH transformed AS (
        SELECT     A.intersection_uid,
            A.datetime_bin,
            A.classification_uid,
            B.leg_new as leg,
            B.dir,
            SUM(A.volume) AS volume,
            array_agg(volume_15min_tmc_uid) as uids

        FROM miovision_api.volumes_15min_tmc A
        INNER JOIN miovision.movement_map B -- TMC to ATR crossover table.
        ON B.leg_old = A.leg AND B.movement_uid = A.movement_uid 
        WHERE A.processed IS NULL
        --AND datetime_bin BETWEEN start_date and end_date
        GROUP BY A.intersection_uid, A.datetime_bin, A.classification_uid, B.leg_new, B.dir
    ),
    --Inserts the ATR bins to the ATR table
    insert_atr AS (
    INSERT INTO miovision_api.volumes_15min(intersection_uid, datetime_bin, classification_uid, leg, dir, volume)
    SELECT intersection_uid, datetime_bin, classification_uid, leg, dir, volume
    FROM transformed
    RETURNING volume_15min_uid, intersection_uid, datetime_bin, classification_uid, leg, dir)
    
    --Updates crossover table with new IDs
    , insert_crossover AS(
    INSERT INTO miovision_api.atr_tmc_uid (volume_15min_tmc_uid, volume_15min_uid)
    SELECT volume_15min_tmc_uid, volume_15min_uid
    FROM insert_atr A
    INNER JOIN (SELECT intersection_uid, datetime_bin, classification_uid, leg, dir, unnest(uids) as volume_15min_tmc_uid FROM transformed) B
        ON A.datetime_bin=B.datetime_bin
        AND A.intersection_uid=B.intersection_uid
        AND A.leg=B.leg
        AND A.classification_uid=B.classification_uid
    ORDER BY volume_15min_uid
	RETURNING volume_15min_tmc_uid
    )
    --Sets processed column to TRUE
    UPDATE miovision_api.volumes_15min_tmc a
    SET processed = TRUE
    FROM insert_crossover b 
    WHERE a.volume_15min_tmc_uid=b.volume_15min_tmc_uid;
    
    RETURN NULL;

END;
$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;