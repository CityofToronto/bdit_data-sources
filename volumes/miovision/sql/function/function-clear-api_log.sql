CREATE OR REPLACE FUNCTION miovision_api.clear_api_log(
    _start_date date,
    _end_date date,
    target_intersection integer DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
VOLATILE
COST 100

AS $BODY$
BEGIN

IF target_intersection IS NULL THEN
    WITH deleted AS (
        DELETE FROM miovision_api.api_log
        WHERE
            start_date >= _start_date
            AND end_date < _end_date
        RETURNING *
    )
    -- FOR NOTICE PURPOSES ONLY
    SELECT COUNT(*) INTO n_deleted
    FROM deleted;

    RAISE NOTICE 'Deleted % rows from miovision_api.api_log.', n_deleted;
        
ELSE 
    WITH deleted AS (
        DELETE FROM miovision_api.api_log
        WHERE
            intersection_uid = target_intersection
            AND start_date >= _start_date
            AND end_date < _end_date;
        RETURNING *
    )
    -- FOR NOTICE PURPOSES ONLY
    SELECT COUNT(*) INTO n_deleted
    FROM deleted;

    RAISE NOTICE 'Deleted % rows from miovision_api.api_log.', n_deleted;

END IF; 

END;

$BODY$;

ALTER FUNCTION miovision_api.clear_api_log(date, date, integer) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.clear_api_log(date, date, integer) TO miovision_api_bot;