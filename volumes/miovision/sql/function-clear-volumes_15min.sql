CREATE OR REPLACE FUNCTION miovision_api.clear_volumes_15min(
    start_date date,
    end_date date)
RETURNS void
LANGUAGE 'plpgsql'
COST 100
VOLATILE
AS $BODY$
BEGIN

    DELETE FROM miovision_api.volumes_15min
    WHERE
        datetime_bin >= start_date
        AND datetime_bin < end_date; 

    UPDATE miovision_api.volumes_15min_mvt
    SET processed = NULL
    WHERE 
        datetime_bin >= start_date
        AND datetime_bin < end_date;

END;
$BODY$;

ALTER FUNCTION miovision_api.clear_volumes_15min(date, date) OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.clear_volumes_15min(date, date) TO miovision_api_bot;
GRANT EXECUTE ON FUNCTION miovision_api.clear_volumes_15min(date, date) TO miovision_admins;