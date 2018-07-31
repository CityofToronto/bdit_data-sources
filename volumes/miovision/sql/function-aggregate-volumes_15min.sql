CREATE OR REPLACE FUNCTION miovision.aggregate_15_min()
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

        FROM miovision.volumes_15min_tmc A
        INNER JOIN miovision.movement_map B ON B.leg_old = A.leg AND B.movement_uid = A.movement_uid
        WHERE 	volume_15min_uid IS NULL
        GROUP BY A.intersection_uid, A.datetime_bin, A.classification_uid, B.leg_new, B.dir
        ORDER BY A.datetime_bin, A.intersection_uid, A.classification_uid, B.leg_new, B.dir
    )
    , aggregate_insert AS(
        INSERT INTO miovision.volumes_15min(intersection_uid, datetime_bin, classification_uid, leg, dir, volume)
        /*If there is no data for a 15-minute bin for a given intersection, classification, leg, direction, there will be no row.
        The first two self join ensure there is a row for every intersection, classification, leg, direction where counting happened 
        at that intersection during that 15-minute bin.
        The final LEFT JOIN ensures that the volume is 0 if there was no data there.
        */
        SELECT A.intersection_uid, B.datetime_bin, A.classification_uid, A.leg, A.dir, COALESCE(C.volume,0) AS volume
        FROM (SELECT intersection_uid, classification_uid, leg, dir FROM transformed GROUP BY intersection_uid, classification_uid, leg, dir) AS A
        INNER JOIN (SELECT intersection_uid, datetime_bin FROM transformed GROUP BY intersection_uid, datetime_bin) AS B USING (intersection_uid)
        LEFT JOIN transformed C USING (intersection_uid, datetime_bin, classification_uid, leg, dir)
        /*Returns the primary key for the volume_15min table and the relevant columns to UPDATE the 15 min tmc table's foreign key*/
        RETURNING volume_15min_uid, intersection_uid, datetime_bin, classification_uid, leg, dir
    )
    UPDATE miovision.volumes_15min_tmc a
    SET volume_15min_uid = b.volume_15min_uid
    /* Subquery matches inserted data with the data aggregated for this insert to have a crossover map between the 
    PRIMARY KEY of volume_15min and the PRIMARY key of volume_15min_tmc*/
    FROM (SELECT unnest(uids) AS volume_15min_tmc_uid, volume_15min_uid 
        FROM transformed 
        INNER JOIN aggregate_insert USING (intersection_uid, datetime_bin, classification_uid, leg, dir)) b
    WHERE a.volume_15min_tmc_uid = b.volume_15min_tmc_uid;
RETURN 1;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE;