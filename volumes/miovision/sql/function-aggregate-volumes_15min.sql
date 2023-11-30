CREATE OR REPLACE FUNCTION miovision_api.aggregate_15_min(
    start_date date,
    end_date date)
RETURNS integer
LANGUAGE 'plpgsql'
COST 100
VOLATILE
AS $BODY$
BEGIN
--Creates the ATR bins
    WITH transformed AS (
        SELECT
            v15.intersection_uid,
            v15.datetime_bin,
            v15.classification_uid,
            mvt.leg_new AS leg,
            mvt.dir,
            SUM(v15.volume) AS volume,
            array_agg(v15.volume_15min_mvt_uid) AS uids
        FROM miovision_api.volumes_15min_mvt AS v15
        INNER JOIN miovision_api.movement_map AS mvt ON -- MVT to ATR crossover table.
            mvt.leg_old = v15.leg
            AND mvt.movement_uid = v15.movement_uid
        WHERE
            v15.processed IS NULL
            AND v15.datetime_bin >= start_date
            AND v15.datetime_bin < end_date
        GROUP BY
            v15.intersection_uid,
            v15.datetime_bin,
            v15.classification_uid,
            mvt.leg_new,
            mvt.dir
    ),

    --Inserts the ATR bins to the ATR table
    insert_atr AS (
        INSERT INTO miovision_api.volumes_15min(intersection_uid, datetime_bin, classification_uid, leg, dir, volume)
        SELECT
            intersection_uid,
            datetime_bin,
            classification_uid,
            leg,
            dir,
            volume
        FROM transformed
        RETURNING volume_15min_uid, intersection_uid, datetime_bin, classification_uid, leg, dir
    ), 
    
    --Updates crossover table with new IDs
    insert_crossover AS(
        INSERT INTO miovision_api.volumes_mvt_atr_xover(volume_15min_mvt_uid, volume_15min_uid)
        SELECT
            volume_15min_mvt_uid,
            volume_15min_uid
        FROM insert_atr AS atr
        INNER JOIN (
            SELECT
                intersection_uid,
                datetime_bin,
                classification_uid,
                leg,
                dir,
                unnest(uids) AS volume_15min_mvt_uid
                FROM transformed
        ) AS ids ON
            atr.intersection_uid = ids.intersection_uid
            AND atr.datetime_bin = ids.datetime_bin
            AND atr.classification_uid = ids.classification_uid
            AND atr.leg = ids.leg
            AND atr.dir = ids.dir
        ORDER BY volume_15min_uid
        RETURNING volume_15min_mvt_uid
    )

    --Sets processed column to TRUE
    UPDATE miovision_api.volumes_15min_mvt AS a
    SET processed = TRUE
    FROM insert_crossover AS b
    WHERE a.volume_15min_mvt_uid = b.volume_15min_mvt_uid;

    RETURN NULL;

EXCEPTION
    WHEN unique_violation THEN
        RAISE EXCEPTION 'Attempting to aggregate data that has already been aggregated but not deleted';
        RETURN 0;
END;
$BODY$;

ALTER FUNCTION miovision_api.aggregate_15_min(date, date)
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min(date, date) TO miovision_api_bot;
GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min(date, date) TO miovision_admins;
