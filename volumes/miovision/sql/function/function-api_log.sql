CREATE OR REPLACE FUNCTION miovision_api.api_log(
    start_date date,
    end_date date,
	intersections integer[] DEFAULT ARRAY[]::integer[]
)
RETURNS void
LANGUAGE plpgsql

COST 100
VOLATILE
AS $BODY$

DECLARE
    target_intersections integer[] =
        CASE WHEN CARDINALITY(intersections) = 0
            --switch out a blank array for all intersections
            THEN (SELECT ARRAY_AGG(intersections.intersection_uid) FROM miovision_api.intersections)
            ELSE intersections
        END;
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
        intersection_uid = ANY(target_intersection)
        AND datetime_bin >= start_date
        AND datetime_bin < end_date
    GROUP BY intersection_uid
    ORDER BY intersection_uid;

END;
$BODY$;

ALTER FUNCTION miovision_api.api_log(date, date, integer) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.api_log(date, date, integer) TO miovision_api_bot;