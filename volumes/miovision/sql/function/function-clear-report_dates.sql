CREATE OR REPLACE FUNCTION miovision_api.clear_report_dates(
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
        DELETE FROM miovision_api.report_dates
        WHERE
            intersection_uid = ANY(target_intersections)
            AND dt >= _start_date
            AND dt < _end_date
        RETURNING *
    )
    -- FOR NOTICE PURPOSES ONLY
    SELECT COUNT(*) INTO n_deleted
    FROM deleted;

    RAISE NOTICE 'Deleted % rows from miovision_api.report_dates.', n_deleted;

END;

$BODY$;

ALTER FUNCTION miovision_api.clear_report_dates(date, date, integer []) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.clear_report_dates(date, date, integer []) TO miovision_api_bot;

COMMENT ON FUNCTION miovision_api.clear_report_dates(date, date, integer [])
IS '''Clears data from `miovision_api.report_dates` in order to facilitate re-pulling.
`intersections` param defaults to all intersections.''';