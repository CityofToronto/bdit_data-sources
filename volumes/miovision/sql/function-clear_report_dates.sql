CREATE OR REPLACE FUNCTION miovision_api.clear_report_dates(
    start_date date,
    end_date date,
    intersection integer default null)
RETURNS VOID
LANGUAGE 'plpgsql'

COST 100
VOLATILE
AS $BODY$

BEGIN

IF intersection IS NULL THEN
    DELETE FROM miovision_api.clear_report_dates
    WHERE
        dt >= start_date
        AND dt < end_date;
ELSE
    DELETE FROM miovision_api.clear_report_dates
    WHERE
        intersection_uid = intersection
        AND dt >= start_date
        AND dt < end_date;
END IF;

END;
$BODY$;

ALTER FUNCTION miovision_api.clear_report_dates(date, date, integer) OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.clear_report_dates(date, date, integer) TO miovision_api_bot;
GRANT EXECUTE ON FUNCTION miovision_api.clear_report_dates(date, date, integer) TO miovision_admins;