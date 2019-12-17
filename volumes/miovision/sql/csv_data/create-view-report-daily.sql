-- View: miovision_api.report_daily

DROP MATERIALIZED VIEW miovision_api.report_daily;

CREATE MATERIALIZED VIEW miovision_api.report_daily
TABLESPACE pg_default
AS
 SELECT intersection_uid,
    intersection_name,
    street_main,
    street_cross,
    class_type,
    dir,
    period_type,
    datetime_bin::date AS dt,
    period_name,
    sum(volume) AS total_volume
   FROM miovision_api.report_volumes_15min 
     CROSS JOIN miovision_api.periods 
     JOIN miovision_api.intersections USING (intersection_uid)
  WHERE datetime_bin::time without time zone <@ period_range AND (period_name = ANY (ARRAY['14 Hour'::text, 'AM Peak Period'::text, 'PM Peak Period'::text])) 
  AND (dir = ANY (ARRAY['EB'::text, 'WB'::text])) 
  AND (street_cross = ANY (ARRAY['Bathurst'::text, 'Spadina'::text, 'Bay'::text, 'Jarvis'::text])) 
  AND (street_cross = 'Bathurst'::text AND (leg = ANY (ARRAY['E'::text, 'S'::text, 'N'::text])) 
	   OR street_cross = 'Jarvis'::text AND (leg = ANY (ARRAY['W'::text, 'S'::text, 'N'::text])) 
	   OR (street_cross <> ALL (ARRAY['Bathurst'::text, 'Jarvis'::text])) AND (dir = 'EB'::text AND (leg = ANY (ARRAY['W'::text, 'N'::text, 'S'::text]))
																			   OR dir = 'WB'::text AND (leg = ANY (ARRAY['E'::text, 'N'::text, 'S'::text]))))
   AND NOT ((class_type = ANY (ARRAY['Vehicles'::text, 'Cyclists'::text])) 
			AND (dir = 'EB'::text AND (street_main = ANY (ARRAY['Wellington'::text, 'Richmond'::text])) 
				 OR dir = 'WB'::text AND street_main = 'Adelaide'::text))
  GROUP BY intersection_uid, intersection_name, street_main, street_cross, period_type, class_type, dir, dt, period_name
WITH NO DATA;

ALTER TABLE miovision_api.report_daily
    OWNER TO miovision_admins;

GRANT ALL ON TABLE miovision_api.report_daily TO bdit_students;
GRANT SELECT, REFERENCES, TRIGGER ON TABLE miovision_api.report_daily TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE miovision_api.report_daily TO bdit_bots;