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
            a.intersection_uid,
            a.datetime_bin,
            a.classification_uid,
            b.leg_new AS leg,
            b.dir,
            SUM(a.volume) AS volume,
            array_agg(a.volume_15min_mvt_uid) AS uids
        FROM miovision_api.volumes_15min_mvt AS a
        INNER JOIN miovision_api.movement_map AS b ON -- MVT to ATR crossover table.
            b.leg_old = a.leg
            AND b.movement_uid = a.movement_uid
        WHERE
            a.processed IS NULL
            AND a.datetime_bin >= start_date
            AND a.datetime_bin < end_date
        -- each day is aggregated from 23:00 the day before to 23:00 of that day
        GROUP BY
            a.intersection_uid,
            a.datetime_bin,
            a.classification_uid,
            b.leg_new,
            b.dir
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
        FROM insert_atr AS a
        INNER JOIN (
            SELECT
                intersection_uid,
                datetime_bin,
                classification_uid,
                leg,
                dir,
                unnest(uids) AS volume_15min_mvt_uid
                FROM transformed
            ) b
            ON a.intersection_uid = b.intersection_uid
            AND a.datetime_bin = b.datetime_bin
            AND a.classification_uid = b.classification_uid
            AND a.leg = b.leg
            AND a.dir = b.dir
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

GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min(date, date) TO bdit_humans WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min(date, date) TO dbadmin WITH GRANT OPTION;
GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min(date, date) TO bdit_bots;
GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min(date, date) TO PUBLIC;
GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min(date, date) TO bdit_humans;
GRANT EXECUTE ON FUNCTION miovision_api.aggregate_15_min(date, date) TO miovision_admins;
