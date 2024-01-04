CREATE OR REPLACE FUNCTION miovision_api.api_log(
    start_date date,
    end_date date,
    intersection integer DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql

COST 100
VOLATILE
AS $BODY$

BEGIN

IF intersection IS NULL
THEN

    INSERT INTO miovision_api.api_log(intersection_uid, start_date, end_date, date_added)
    SELECT
        intersection_uid,
        MIN(datetime_bin::date),
        MAX(datetime_bin::date),
        current_timestamp::date
    FROM miovision_api.volumes
    WHERE
        datetime_bin >= start_date
        AND datetime_bin < end_date
    GROUP BY intersection_uid
    ORDER BY intersection_uid;

ELSE

    INSERT INTO miovision_api.api_log(intersection_uid, start_date, end_date, date_added)
    SELECT
        intersection_uid,
        MIN(datetime_bin::date),
        MAX(datetime_bin::date),
        current_timestamp::date
    FROM miovision_api.volumes
    WHERE
        intersection_uid = intersection
        AND datetime_bin >= start_date
        AND datetime_bin < end_date
    GROUP BY intersection_uid
    ORDER BY intersection_uid;

END IF;

END;
$BODY$;

ALTER FUNCTION miovision_api.api_log(date, date, integer) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.api_log(date, date, integer) TO miovision_api_bot;