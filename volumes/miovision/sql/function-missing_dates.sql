-- FUNCTION: miovision_api.missing_dates(date)

-- DROP FUNCTION miovision_api.missing_dates(date);

CREATE OR REPLACE FUNCTION miovision_api.missing_dates(
	_date date)
    RETURNS integer
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$
BEGIN

WITH find_dates AS(
SELECT intersection_uid 
FROM (SELECT A.intersection_uid, B.datetime_bin 
	  FROM miovision_api.intersections 
	  LEFT OUTER JOIN miovision_api.volumes_15min USING(intersection_uid)
	  WHERE datetime_bin BETWEEN _date AND _date + INTERVAL '1 Day' OR datetime_bin IS NULL ) a WHERE datetime_bin IS NULL
) 

INSERT INTO miovision_api.missing_dates 
SELECT find_dates.intersection_uid, _date, to_char(date_trunc('month', _date),'Mon YYYY')
FROM find_dates;

RETURN 1;
END;
$BODY$;

ALTER FUNCTION miovision_api.missing_dates(date)
    OWNER TO miovision_admins;
