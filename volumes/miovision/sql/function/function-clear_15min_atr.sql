CREATE OR REPLACE FUNCTION miovision_api.clear_15_min_atr(
    start_date timestamp,
    end_date timestamp,
    intersections integer [] DEFAULT ARRAY[]::integer []
)
RETURNS void
LANGUAGE plpgsql

COST 100
VOLATILE
AS $BODY$

DECLARE
    target_intersections integer [] = miovision_api.get_intersections_uids(intersections);
    n_deleted integer;

BEGIN

    WITH aggregate_delete AS (
        DELETE FROM miovision_api.volumes_15min_atr_unfiltered_table
        WHERE
            intersection_uid = ANY(target_intersections)
            AND datetime_bin >= start_date
            AND datetime_bin < end_date
        RETURNING intersection_uid
    )
    
    -- FOR NOTICE PURPOSES ONLY
    SELECT COUNT(*) INTO n_deleted
    FROM aggregate_delete;

    RAISE NOTICE 'Deleted % rows from miovision_api.volumes_15min_atr_unfiltered_table.', n_deleted;

END;

$BODY$;

ALTER FUNCTION miovision_api.clear_15_min_atr(timestamp, timestamp, integer [])
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.clear_15_min_atr(timestamp, timestamp, integer [])
TO miovision_api_bot;

GRANT EXECUTE ON FUNCTION miovision_api.clear_15_min_atr(timestamp, timestamp, integer [])
TO miovision_admins;

COMMENT ON FUNCTION miovision_api.clear_15_min_atr(timestamp, timestamp, integer [])
IS '''Clears data from `miovision_api.volumes_15min_atr_unfiltered_table` in order to facilitate
re-pulling. `intersections` param defaults to all intersections.''';
