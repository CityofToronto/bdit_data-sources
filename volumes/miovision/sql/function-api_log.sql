-- DROP FUNCTION miovision_api_test.api_log(timestamp without time zone, timestamp without time zone);

CREATE OR REPLACE FUNCTION miovision_api_test.api_log(
    start_date timestamp without time zone,
    end_date timestamp without time zone)
  RETURNS integer AS
$BODY$
BEGIN


INSERT INTO miovision_api_test.api_log (intersection_uid, start_date, end_date, date_added)
SELECT intersection_uid, MAX(datetime_bin::date), MIN(datetime_bin::date), current_timestamp::date from miovision_api_test.volumes
WHERE datetime_bin BETWEEN start_date AND end_date
GROUP BY intersection_uid
ORDER BY now, intersection_uid;

	RETURN NULL;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;