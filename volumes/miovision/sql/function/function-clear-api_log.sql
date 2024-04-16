CREATE OR REPLACE FUNCTION miovision_api.clear_api_log(
    _start_date date,
    _end_date date,
    intersections integer [] DEFAULT ARRAY[]::integer []
)
RETURNS void
LANGUAGE plpgsql
VOLATILE
COST 100

AS $BODY$

DECLARE
    target_intersections integer [] = miovision_api.get_intersections_uids(intersections);
    n_deleted integer;

BEGIN

    WITH deleted AS (
        DELETE FROM miovision_api.api_log
        WHERE
            intersection_uid = ANY(target_intersections)
            AND start_date >= _start_date
            AND end_date < _end_date
        RETURNING *
    )
    -- FOR NOTICE PURPOSES ONLY
    SELECT COUNT(*) INTO n_deleted
    FROM deleted;

    RAISE NOTICE 'Deleted % rows from miovision_api.api_log.', n_deleted;

END;

$BODY$;

ALTER FUNCTION miovision_api.clear_api_log(date, date, integer []) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.clear_api_log(date, date, integer []) TO miovision_api_bot;

COMMENT ON FUNCTION miovision_api.clear_api_log(date, date, integer [])
IS '''Clears data from `miovision_api.api_log` in order to facilitate re-pulling.
`intersections` param defaults to all intersections.''';