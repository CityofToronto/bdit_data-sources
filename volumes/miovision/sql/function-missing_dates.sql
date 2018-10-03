CREATE OR REPLACE FUNCTION miovision_api.missing_dates(
    start_date date,
    end_date date)
  RETURNS integer AS
$BODY$
BEGIN

WITH find_dates AS(
SELECT intersection_uid FROM (SELECT A.intersection_uid, B.datetime_bin from miovision.intersections a
LEFT OUTER JOIN miovision_api.volumes_15min b ON a.intersection_uid=b.intersection_uid
WHERE datetime_bin BETWEEN start_date AND end_date OR datetime_bin IS NULL ) a WHERE datetime_bin IS NULL
) 

INSERT INTO miovision_api.missing_dates 
SELECT find_dates.intersection_uid, start_date, to_char(date_trunc('month',start_date),'Mon YYYY')
FROM find_dates;

RETURN 1;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;