-- View: open_data.miovision_2017

-- DROP VIEW open_data.miovision_2017;
-- 
CREATE OR REPLACE VIEW open_data.miovision_2017 AS 
 SELECT period_type as aggregation_period,
    int_id,
    intersec5 AS intersection_name,
    px,
    volumes_15min.datetime_bin,
    CASE WHEN classification ='Buses' THEN 'Buses and streetcars'
    ELSE classification END AS classification,
    volumes_15min.leg,
    volumes_15min.dir,
    SUM(volumes_15min.volume)::INT as volume

   FROM miovision.volumes_15min 
   INNER JOIN miovision.intersections USING (intersection_uid)
   INNER JOIN gis.centreline_intersection USING (int_id)
   INNER JOIN miovision.classifications USING(classification_uid)
   INNER JOIN miovision_new.report_dates_view a USING (intersection_uid, class_type_id)
   LEFT OUTER JOIN miovision_new.exceptions e ON (a.intersection_uid, a.class_type_id) =(e.intersection_uid, e.class_type_id) AND datetime_bin <@ excluded_datetime
  WHERE dt = datetime_bin::DATE AND date_part('year'::text, volumes_15min.datetime_bin) = 2017::double precision
    AND exceptions_uid IS NULL --exclude excepted data
GROUP BY int_id, intersec5, px, datetime_bin, classification, leg, dir, period_type;
ALTER TABLE open_data.miovision_2017
  OWNER TO rdumas;
GRANT ALL ON TABLE open_data.miovision_2017 TO rdumas;
GRANT SELECT ON TABLE open_data.miovision_2017 TO od_extract_svc;

-- DROP VIEW open_data.miovision_2018;

CREATE OR REPLACE VIEW open_data.miovision_2018 AS 
 SELECT period_type as aggregation_period,
    int_id,
    intersec5 AS intersection_name,
    px,
    volumes_15min.datetime_bin,
    CASE WHEN classification ='Buses' THEN 'Buses and streetcars'
    ELSE classification END AS classification,
    volumes_15min.leg,
    volumes_15min.dir,
    SUM(volumes_15min.volume)::INT as volume
  FROM miovision.volumes_15min 
   INNER JOIN miovision.intersections USING (intersection_uid)
   INNER JOIN gis.centreline_intersection USING (int_id)
   INNER JOIN miovision.classifications USING(classification_uid)
   INNER JOIN miovision_new.report_dates_view a USING (intersection_uid, class_type_id)  
   LEFT OUTER JOIN miovision_new.exceptions e ON (a.intersection_uid, a.class_type_id) =(e.intersection_uid, e.class_type_id) AND datetime_bin <@ excluded_datetime
  WHERE dt = datetime_bin::DATE AND datetime_bin >= '2018-01-01'
  AND datetime_bin < LEAST(date_trunc('month', current_date) - INTERVAL '1 Month', '2018-01-01'::DATE + INTERVAL '1 Year') 
    AND exceptions_uid IS NULL --exclude excepted data
  GROUP BY int_id, intersec5, px, datetime_bin, classification, leg, dir, period_type;
ALTER TABLE open_data.miovision_2018
  OWNER TO rdumas;
GRANT ALL ON TABLE open_data.miovision_2018 TO rdumas;
GRANT SELECT ON TABLE open_data.miovision_2018 TO od_extract_svc;


CREATE OR REPLACE VIEW open_data.miovision_2019 AS
 SELECT a.period_type AS aggregation_period,
    intersections.int_id,
    centreline_intersection.intersec5 AS intersection_name,
    intersections.px,
    volumes_15min.datetime_bin,
        CASE
            WHEN classifications.classification = 'Buses'::text THEN 'Buses and streetcars'::text
            ELSE classifications.classification
        END AS classification,
    volumes_15min.leg,
    volumes_15min.dir,
    sum(volumes_15min.volume)::integer AS volume
   FROM miovision_new.volumes_15min
     JOIN miovision_new.intersections USING (intersection_uid)
     JOIN gis.centreline_intersection USING (int_id)
     JOIN miovision_new.classifications USING (classification_uid)
     JOIN miovision_new.report_dates_view a USING (intersection_uid, class_type_id)
     LEFT JOIN miovision_new.exceptions e ON a.intersection_uid = e.intersection_uid AND a.class_type_id = e.class_type_id AND volumes_15min.datetime_bin <@ e.excluded_datetime
  WHERE date_part('year'::text, volumes_15min.datetime_bin) = 2019::double precision 
  AND volumes_15min.datetime_bin < LEAST(date_trunc('month'::text, now) - '1 mon'::interval,
                                         ('2019-01-01'::date + '1 year'::interval)::timestamp with time zone) 
  AND a.dt = volumes_15min.datetime_bin::date AND e.exceptions_uid IS NULL
  GROUP BY intersections.int_id, centreline_intersection.intersec5, intersections.px, volumes_15min.datetime_bin, classifications.classification, volumes_15min.leg, volumes_15min.dir, a.period_type;

ALTER TABLE open_data.miovision_2019
    OWNER TO rdumas;

GRANT SELECT ON TABLE open_data.miovision_2019 TO od_extract_svc;
GRANT ALL ON TABLE open_data.miovision_2019 TO rdumas;
GRANT SELECT ON TABLE open_data.miovision_2019 TO bdit_humans;




-- View: open_data.ksp_miovision_summary

DROP VIEW open_data.ksp_miovision_summary;

CREATE OR REPLACE VIEW open_data.ksp_miovision_summary AS 
WITH valid_bins AS (
         SELECT a_1.intersection_uid,
            a_1.class_type_id,
            a_1.dt + b_1.b::time without time zone AS datetime_bin,
            a_1.period_type,
            int_id,
            px,
            intersection_name,
            street_main,
            street_cross
          FROM miovision.report_dates_view a_1
            JOIN miovision.intersections USING (intersection_uid)
            CROSS JOIN (SELECT generate_series('2017-01-01 00:00:00'::timestamp without time zone, '2017-01-01 23:45:00'::timestamp without time zone, '00:15:00'::interval)::TIME) b_1(b)
            LEFT JOIN miovision_new.exceptions c ON a_1.intersection_uid = c.intersection_uid AND a_1.class_type_id = c.class_type_id AND (a_1.dt + b_1.b::time without time zone) <@ c.excluded_datetime
             
          WHERE c.exceptions_uid IS NULL AND street_cross IN ('Bathurst', 'Spadina', 'Bay', 'Jarvis')
          AND a_1.dt < date_trunc('month', current_date) - INTERVAL '1 Month'
        )
 , volumes_15 AS (
 
 SELECT a.intersection_uid,
    a.period_type,
    a.datetime_bin,
    datetime_bin::DATE dt, 
    class_type,
    b.dir,
    b.leg,
    COALESCE(c.total_volume, b.avg_volume) AS volume,
    int_id,
    px,
    intersection_name,
    street_main,
    street_cross
  FROM valid_bins a
     JOIN miovision.int_time_bin_avg b USING (intersection_uid, class_type_id, period_type)
     INNER JOIN miovision.class_types USING (class_type_id)
     LEFT JOIN miovision.volumes_15min_by_class c USING (datetime_bin, intersection_uid, class_type_id, dir, leg, period_type)
  WHERE b.time_bin = a.datetime_bin::time without time zone
)
, intersection_days as(
  SELECT intersection_uid,
         dt,
         COUNT(DISTINCT datetime_bin) AS num_daily_observations
	FROM volumes_15
	GROUP BY intersection_uid, dt)
, daily AS(
 SELECT a.intersection_uid,
    int_id,
    px,
    class_type,
    a.dir,
    a.period_type,
    a.datetime_bin::date AS dt2,
    b.period_name || 
    CASE WHEN period_id IN (4,8) THEN '' --24 hour
    ELSE --Append period range 
	(((' ('::text || to_char(lower(b.period_range)::interval, 'HH24:MM'::text)) || '-'::text) || to_char(upper(b.period_range)::interval, 'HH24:MM'::text)) || ')'::text 
    END
    AS "period_name",
    sum(a.volume) AS total_volume
   FROM volumes_15 a
     CROSS JOIN miovision.periods b
     JOIN intersection_days d USING (intersection_uid, dt)

  WHERE a.datetime_bin::time without time zone <@ b.period_range 
    AND report_flag AND (a.dir = ANY (ARRAY['EB'::text, 'WB'::text])) 
    AND (period_id NOT IN (4,8)
        OR period_id = 8 AND num_daily_observations > 23 * 4 --Make sure 24hour counts have data for most of the 24-hour period
        OR period_id = 4 AND num_daily_observations > 13 * 4 --Make sure 14hour counts have data for most of the 14-hour period
        )
     
    AND (street_cross = 'Bathurst' AND (a.leg IN ('E', 'S', 'N')) 
        OR street_cross = 'Jarvis' AND (a.leg IN ('W', 'S', 'N')) 
        OR (street_cross NOT IN ('Bathurst', 'Jarvis')) AND (a.dir = 'EB' AND (a.leg IN ('W', 'N', 'S')) 
        OR a.dir = 'WB' AND (a.leg IN ('E', 'N', 'S')))) AND NOT ((class_type IN ('Vehicles', 'Cyclists')) AND (a.dir = 'EB' AND (street_main IN ('Wellington', 'Richmond'))
        OR a.dir = 'WB' AND street_main = 'Adelaide'))
  GROUP BY a.intersection_uid, int_id, px, a.period_type, class_type, a.dir, dt2, b.period_name, b.period_range, period_id
   
)
SELECT period_type as aggregation_period, int_id,
    centreline_intersection.intersec5 AS intersection_name, px,
    CASE
            WHEN class_type = 'Buses'::text THEN 'Buses and streetcars'::text
            ELSE class_type
        END AS classification,
        dir, period_name, AVG(total_volume)::int as volume
FROM daily
JOIN gis.centreline_intersection USING (int_id)
GROUP BY period_type, int_id, class_type, intersec5, px, dir, period_name;
GRANT SELECT ON TABLE open_data.ksp_miovision_summary TO od_extract_svc;
