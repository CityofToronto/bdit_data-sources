CREATE OR REPLACE FUNCTION miovision_api.get_report_dates(
    start_date timestamp without time zone,
    end_date timestamp without time zone)
RETURNS integer
AS $BODY$

BEGIN

INSERT INTO miovision_api.report_dates

WITH classes(y) AS (
	SELECT UNNEST(a) FROM ( VALUES(ARRAY['Vehicles','Pedestrians','Cyclists'])) x(a)
)

SELECT intersection_uid, classes.y AS class_type, CASE WHEN datetime_bin <= '2017-11-11' THEN 'Baseline' ELSE to_char(date_trunc('month',datetime_bin),'Mon YYYY') END AS period_type, datetime_bin::date AS dt
FROM miovision_api.volumes_15min
INNER JOIN miovision_api.intersections USING (intersection_uid)
CROSS JOIN classes
WHERE datetime_bin::time >= '06:00' AND datetime_bin::time < '20:00' AND EXTRACT(isodow FROM datetime_bin) <= 5
AND datetime_bin BETWEEN start_date AND end_date
GROUP BY intersection_uid, classes.y, datetime_bin::date, CASE WHEN datetime_bin <= '2017-11-11' THEN 'Baseline' ELSE to_char(date_trunc('month',datetime_bin),'Mon YYYY') END
HAVING COUNT(DISTINCT datetime_bin::time) >= 40
ON CONFLICT DO NOTHING;



RETURN 1;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;


ALTER FUNCTION miovision_api.report_dates(timestamp without time zone, timestamp without time zone)
    OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.report_dates(timestamp without time zone, timestamp without time zone) TO dbadmin WITH GRANT OPTION;

GRANT EXECUTE ON FUNCTION miovision_api.report_dates(timestamp without time zone, timestamp without time zone) TO miovision_api_bot;

GRANT EXECUTE ON FUNCTION miovision_api.report_dates(timestamp without time zone, timestamp without time zone) TO PUBLIC;

GRANT EXECUTE ON FUNCTION miovision_api.report_dates(timestamp without time zone, timestamp without time zone) TO miovision_admins;