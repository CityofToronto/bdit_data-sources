CREATE OR REPLACE FUNCTION miovision_api.clear_volumes(
    start_date timestamp,
    end_date timestamp,
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
        DELETE FROM miovision_api.volumes
        WHERE
            intersection_uid = ANY(target_intersections)
            AND datetime_bin >= start_date
            AND datetime_bin < end_date
        RETURNING *
    )
    -- FOR NOTICE PURPOSES ONLY
    SELECT COUNT(*) INTO n_deleted
    FROM deleted;

    RAISE INFO 'Deleted % rows from miovision_api.volumes.', n_deleted;

END;

$BODY$;

ALTER FUNCTION miovision_api.clear_volumes(timestamp, timestamp, integer [])
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.clear_volumes(timestamp, timestamp, integer [])
TO miovision_api_bot;

COMMENT ON FUNCTION miovision_api.clear_volumes(timestamp, timestamp, integer [])
IS '''Clears data from `miovision_api.volumes` in order to facilitate re-pulling.
`intersections` param defaults to all intersections.''';