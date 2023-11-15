CREATE OR REPLACE FUNCTION miovision_api.clear_15_min_mvt(
    start_date date,
    end_date date,
    intersection integer default null)
RETURNS void
LANGUAGE 'plpgsql'

COST 100
VOLATILE
AS $BODY$

BEGIN

IF intersection IS NULL
THEN

    WITH aggregate_delete AS (
        DELETE FROM miovision_api.volumes_15min_mvt
        WHERE
            datetime_bin >= start_date - interval '1 hour'
            AND datetime_bin < end_date - interval '1 hour'
        RETURNING volume_15min_mvt_uid
    )

    --To update foreign key for 1min bin table
    UPDATE miovision_api.volumes AS a
    SET volume_15min_mvt_uid = NULL
    FROM aggregate_delete AS b
    WHERE
        a.volume_15min_mvt_uid = b.volume_15min_mvt_uid
        AND a.datetime_bin >= start_date - interval '1 hour'
        AND a.datetime_bin < end_date - interval '1 hour';

ELSE

    WITH aggregate_delete AS (
        DELETE FROM miovision_api.volumes_15min_mvt
        WHERE
            intersection_uid = intersection
            AND datetime_bin >= start_date - interval '1 hour'
            AND datetime_bin < end_date - interval '1 hour'
        RETURNING volume_15min_mvt_uid
    )
    
    --To update foreign key for 1min bin table
    UPDATE miovision_api.volumes AS a
    SET volume_15min_mvt_uid = NULL
    FROM aggregate_delete AS b
    WHERE
        a.intersection_uid = intersection
        AND a.volume_15min_mvt_uid = b.volume_15min_mvt_uid
        AND a.datetime_bin >= start_date - interval '1 hour'
        AND a.datetime_bin < end_date - interval '1 hour';

END IF;

END;

$BODY$;

ALTER FUNCTION miovision_api.clear_15_min_mvt(date, date, integer) OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.clear_15_min_mvt(date, date, integer) TO miovision_api_bot;
GRANT EXECUTE ON FUNCTION miovision_api.clear_15_min_mvt(date, date, integer) TO miovision_admins;
