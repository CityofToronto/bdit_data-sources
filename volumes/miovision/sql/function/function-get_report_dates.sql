CREATE OR REPLACE FUNCTION miovision_api.get_report_dates(
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    intersections integer [] DEFAULT ARRAY[]::integer []
)
RETURNS void
AS $BODY$
BEGIN

DECLARE 
    target_intersections integer [] = miovision_api.get_intersections_uids(intersections);

INSERT INTO miovision_api.report_dates
SELECT
    i.intersection_uid,
    classes.y AS class_type,
    CASE WHEN v.datetime_bin <= '2017-11-11'
        THEN 'Baseline'
        ELSE to_char(date_trunc('month', v.datetime_bin),'Mon YYYY')
    END AS period_type,
    v.datetime_bin::date AS dt
FROM miovision_api.volumes_15min AS v
INNER JOIN miovision_api.intersections AS i USING (intersection_uid)
CROSS JOIN (VALUES ('Vehicles'), ('Pedestrians'), ('Cyclists')) AS classes(y)
WHERE
    v.datetime_bin::time >= '06:00'
    AND v.datetime_bin::time < '20:00'
    AND extract(isodow FROM v.datetime_bin) <= 5
    AND v.datetime_bin BETWEEN start_date AND end_date
    AND i.intersection_uid = ANY(target_intersections)
GROUP BY
    i.intersection_uid,
    classes.y,
    dt,
    period_type
HAVING COUNT(DISTINCT v.datetime_bin::time) >= 40
ON CONFLICT DO NOTHING;

END;
$BODY$

LANGUAGE plpgsql
VOLATILE
COST 100;

ALTER FUNCTION miovision_api.report_dates(timestamp, timestamp)
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.report_dates(timestamp, timestamp) TO miovision_api_bot;