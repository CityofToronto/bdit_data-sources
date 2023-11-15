CREATE OR REPLACE FUNCTION miovision_api.clear_volumes_15min(
    start_date date,
    end_date date,
    intersection integer default null)
RETURNS void
LANGUAGE 'plpgsql'
COST 100
VOLATILE
AS $BODY$
BEGIN

IF intersection IS NULL THEN

    DELETE FROM miovision_api.volumes_15min
    WHERE
        datetime_bin >= start_date - interval '1 hour'
        AND datetime_bin < end_date - interval '1 hour'; 

    UPDATE miovision_api.volumes_15min_mvt
    SET processed = NULL
    WHERE 
        datetime_bin >= start_date - interval '1 hour'
        AND datetime_bin < end_date - interval '1 hour';

ELSE

    DELETE FROM miovision_api.volumes_15min
    WHERE
        intersection_uid = intersection
        AND datetime_bin >= start_date - interval '1 hour'
        AND datetime_bin < end_date - interval '1 hour';

    UPDATE miovision_api.volumes_15min_mvt
    SET processed = NULL
    WHERE
        intersection_uid = intersection
        AND datetime_bin >= start_date - interval '1 hour'
        AND datetime_bin < end_date - interval '1 hour';

END IF;

END;
$BODY$;

ALTER FUNCTION miovision_api.clear_volumes_15min(date, date, integer) OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.clear_volumes_15min(date, date, integer) TO miovision_api_bot;
GRANT EXECUTE ON FUNCTION miovision_api.clear_volumes_15min(date, date, integer) TO miovision_admins;
