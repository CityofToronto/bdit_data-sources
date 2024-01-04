CREATE OR REPLACE FUNCTION miovision_api.clear_15_min_mvt(
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
    target_intersections integer [] =
        CASE WHEN CARDINALITY(intersections) = 0
            --switch out a blank array for all intersections
            THEN (SELECT ARRAY_AGG(intersections.intersection_uid) FROM miovision_api.intersections)
            ELSE intersections
        END;
    n_deleted integer;

BEGIN

    WITH aggregate_delete AS (
        DELETE FROM miovision_api.volumes_15min_mvt
        WHERE
            intersection_uid = intersection
            AND datetime_bin >= start_date
            AND datetime_bin < end_date
        RETURNING volume_15min_mvt_uid
    ),
    
    updated AS (
        --To update foreign key for 1min bin table
        UPDATE miovision_api.volumes AS a
        SET volume_15min_mvt_uid = NULL
        FROM aggregate_delete AS b
        WHERE
            a.intersection_uid = ANY(target_intersection)
            AND a.volume_15min_mvt_uid = b.volume_15min_mvt_uid
            AND a.datetime_bin >= start_date
            AND a.datetime_bin < end_date
    )

    -- FOR NOTICE PURPOSES ONLY
    SELECT COUNT(*) INTO n_deleted
    FROM aggregate_delete;

    RAISE NOTICE 'Deleted % rows from miovision_api.volumes_15min_mvt.', n_deleted;

END;

$BODY$;

ALTER FUNCTION miovision_api.clear_15_min_mvt(timestamp, timestamp, integer [])
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.clear_15_min_mvt(timestamp, timestamp, integer [])
TO miovision_api_bot;

GRANT EXECUTE ON FUNCTION miovision_api.clear_15_min_mvt(timestamp, timestamp, integer [])
TO miovision_admins;

COMMENT ON FUNCTION miovision_api.clear_15_min_mvt(timestamp, timestamp, integer [])
IS 'Clears data from miovision_api.volumes_15min_mvt in order to facilitate re-pulling.
Intersections value defaults to all intersections.'