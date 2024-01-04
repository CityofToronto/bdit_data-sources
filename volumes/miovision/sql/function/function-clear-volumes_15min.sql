CREATE OR REPLACE FUNCTION miovision_api.clear_volumes_15min(
    start_date timestamp,
    end_date timestamp
)
RETURNS void
LANGUAGE 'plpgsql'
COST 100
VOLATILE
AS $BODY$

DECLARE
    n_deleted integer;

BEGIN

    WITH deleted AS (
        DELETE FROM miovision_api.volumes_15min
        WHERE
            datetime_bin >= start_date
            AND datetime_bin < end_date
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
        AND datetime_bin < end_date;

END;
$BODY$;

ALTER FUNCTION miovision_api.clear_volumes_15min(timestamp, timestamp) OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.clear_volumes_15min(timestamp, timestamp)
TO miovision_api_bot;

GRANT EXECUTE ON FUNCTION miovision_api.clear_volumes_15min(timestamp, timestamp)
TO miovision_admins;