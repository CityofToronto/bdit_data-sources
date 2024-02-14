CREATE OR REPLACE FUNCTION miovision_api.api_log(
    start_date date,
    end_date date,
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

    INSERT INTO miovision_api.api_log(intersection_uid, start_date, end_date, date_added)
    SELECT
        intersection_uid,
        MIN(datetime_bin::date),
        MAX(datetime_bin::date),
        current_timestamp::date
    FROM miovision_api.volumes
    WHERE
        intersection_uid = ANY(target_intersections)
        AND datetime_bin >= start_date
        AND datetime_bin < end_date
    GROUP BY intersection_uid
    ORDER BY intersection_uid;

END;
$BODY$;

ALTER FUNCTION miovision_api.api_log(date, date, integer []) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.api_log(date, date, integer []) TO miovision_api_bot;

COMMENT ON FUNCTION miovision_api.api_log(date, date, integer [])
IS '''Logs inserts from the api to miovision_api.volumes via the `miovision_api.api_log` table.
Takes an optional intersection array parameter to aggregate only specific intersections.
Use `clear_api_log()` to remove existing values before summarizing.''';