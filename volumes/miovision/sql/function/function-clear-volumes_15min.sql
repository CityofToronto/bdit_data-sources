CREATE OR REPLACE FUNCTION miovision_api.clear_volumes_15min(
    start_date timestamp,
    end_date timestamp,
    intersections integer [] DEFAULT ARRAY[]::integer []
)
RETURNS void
LANGUAGE 'plpgsql'
COST 100
VOLATILE
AS $BODY$

DECLARE
    n_deleted integer;
    target_intersections integer [] = miovision_api.get_intersections_uids(intersections);

BEGIN

    WITH deleted AS (
        DELETE FROM miovision_api.volumes_15min
        WHERE
            datetime_bin >= start_date
            AND datetime_bin < end_date
            AND intersection_uid = ANY(target_intersections)
        RETURNING *
    )

    -- FOR NOTICE PURPOSES ONLY
    SELECT COUNT(*) INTO n_deleted
    FROM deleted;

    RAISE NOTICE 'Deleted % rows from miovision_api.volumes_15min.', n_deleted;

    UPDATE miovision_api.volumes_15min_mvt
    SET processed = NULL
    WHERE
        datetime_bin >= start_date
        AND datetime_bin < end_date
        AND intersection_uid = ANY(target_intersections);

END;
$BODY$;

ALTER FUNCTION miovision_api.clear_volumes_15min(timestamp, timestamp) OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.clear_volumes_15min(timestamp, timestamp)
TO miovision_api_bot;

GRANT EXECUTE ON FUNCTION miovision_api.clear_volumes_15min(timestamp, timestamp)
TO miovision_admins;

COMMENT ON FUNCTION miovision_api.clear_volumes_15min(timestamp, timestamp, integer []) IS
'''Clears data from `miovision_api.volumes_15min` in order to facilitate re-pulling.
`intersections` param defaults to all intersections.''';