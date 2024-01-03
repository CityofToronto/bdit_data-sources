CREATE OR REPLACE FUNCTION miovision_api.api_log(
    start_date date,
    end_date date)
  RETURNS integer AS
$BODY$
BEGIN


INSERT INTO miovision_api.api_log (intersection_uid, start_date, end_date, date_added)
SELECT intersection_uid,  MIN(datetime_bin::date), MAX(datetime_bin::date), current_timestamp::date from miovision_api.volumes
WHERE datetime_bin BETWEEN start_date AND end_date
GROUP BY intersection_uid
ORDER BY current_timestamp::date, intersection_uid;

	RETURN NULL;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION miovision_api.api_log(date, date)
  OWNER TO miovision_admins;