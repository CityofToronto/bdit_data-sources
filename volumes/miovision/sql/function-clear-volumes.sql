﻿CREATE OR REPLACE FUNCTION miovision_api.clear_volumes(
    start_date timestamp,
    end_date timestamp,
    intersection integer DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql
VOLATILE
COST 100

AS $BODY$
BEGIN

IF intersection IS NULL THEN
    DELETE FROM miovision_api.volumes
    WHERE
        datetime_bin >= start_date
        AND datetime_bin < end_date;
ELSE 
    DELETE FROM miovision_api.volumes
    WHERE
        intersection_uid = intersection
        AND datetime_bin >= start_date
        AND datetime_bin < end_date;
END IF; 

RETURN NULL;
END;

$BODY$;

ALTER FUNCTION miovision_api.clear_volumes(date, date, integer) OWNER TO miovision_admins;
GRANT EXECUTE ON FUNCTION miovision_api.clear_volumes(date, date, integer) TO miovision_api_bot;