DROP MATERIALIZED VIEW rliu.ksp_miovision_summary_new;
CREATE MATERIALIZED VIEW rliu.ksp_miovision_summary_new AS 
 WITH valid_bins AS (
         SELECT a_1.intersection_uid,
            a_1.class_type,
            a_1.dt + b_1.b AS datetime_bin,
            a_1.period_type

           FROM miovision_new.report_dates a_1
          CROSS JOIN ( SELECT generate_series('2017-01-01 00:00:00'::timestamp without time zone, '2017-01-01 23:45:00'::timestamp without time zone, '00:15:00'::interval)::time without time zone AS generate_series) b_1(b)
         LEFT JOIN miovision.exceptions c ON 
           a_1.intersection_uid=c.intersection_uid AND
         a_1.class_type =c.class_type
         AND (a_1.dt + b_1.b )<@ c.excluded_datetime
         WHERE excluded_datetime IS NULL
          ORDER BY a_1.intersection_uid, a_1.class_type, (a_1.dt + b_1.b)
        ), int_avg AS (
         SELECT volumes_15min_by_class.intersection_uid,
            volumes_15min_by_class.class_type,
            volumes_15min_by_class.dir,
            volumes_15min_by_class.leg,
            volumes_15min_by_class.period_type,
            volumes_15min_by_class.datetime_bin::time without time zone AS time_bin,
            avg(volumes_15min_by_class.total_volume) AS avg_volume
           FROM miovision_new.volumes_15min_by_class
          GROUP BY volumes_15min_by_class.intersection_uid, volumes_15min_by_class.class_type, volumes_15min_by_class.period_type, volumes_15min_by_class.dir, volumes_15min_by_class.leg, (volumes_15min_by_class.datetime_bin::time without time zone)
        ), volumes_15 AS (
         SELECT a.intersection_uid,
            a.period_type,
            a.datetime_bin,
            a.class_type,
            b.dir,
            b.leg,
            COALESCE(c.total_volume, b.avg_volume) AS volume
           FROM valid_bins a
             JOIN int_avg b USING (intersection_uid, class_type, period_type)
             LEFT JOIN miovision_new.volumes_15min_by_class c USING (datetime_bin, intersection_uid, class_type, dir, leg, period_type)
          WHERE b.time_bin = a.datetime_bin::time without time zone
        
          ORDER BY a.intersection_uid, a.period_type, a.datetime_bin, a.class_type, b.dir, b.leg
        ), intersection_days AS (
         SELECT volumes_15.intersection_uid,
            volumes_15.datetime_bin::date AS dt,
            count(DISTINCT volumes_15.datetime_bin) AS num_daily_observations
           FROM volumes_15
          GROUP BY volumes_15.intersection_uid, (volumes_15.datetime_bin::date)
        ), daily AS (
         SELECT a.intersection_uid,
            c.intersection_name,
            c.street_main,
            c.street_cross,
            a.class_type,
            a.dir,
            a.period_type,
            a.datetime_bin::date AS dt,
            b.period_name ||
                CASE
                    WHEN b.period_id = 7 THEN ''::text
                    ELSE (((' ('::text || to_char(lower(b.period_range)::interval, 'HH24:MM'::text)) || '-'::text) || to_char(upper(b.period_range)::interval, 'HH24:MM'::text)) || ')'::text
                END AS period_name,
            sum(a.volume) AS total_volume
           FROM volumes_15 a
             CROSS JOIN miovision_new.periods b
             JOIN miovision_new.intersections c USING (intersection_uid)
             JOIN intersection_days d ON d.intersection_uid = a.intersection_uid AND a.datetime_bin::date = d.dt
          WHERE a.datetime_bin::time without time zone <@ b.period_range AND b.report_flag  AND (a.dir = ANY (ARRAY['EB'::text, 'WB'::text])) AND (b.period_id <> 7 OR d.num_daily_observations > (23 * 4)) AND (c.street_cross = ANY (ARRAY['Bathurst'::text, 'Spadina'::text, 'Bay'::text, 'Jarvis'::text])) AND (c.street_cross = 'Bathurst'::text AND (a.leg = ANY (ARRAY['E'::text, 'S'::text, 'N'::text])) OR c.street_cross = 'Jarvis'::text AND (a.leg = ANY (ARRAY['W'::text, 'S'::text, 'N'::text])) OR (c.street_cross <> ALL (ARRAY['Bathurst'::text, 'Jarvis'::text])) AND (a.dir = 'EB'::text AND (a.leg = ANY (ARRAY['W'::text, 'N'::text, 'S'::text])) OR a.dir = 'WB'::text AND (a.leg = ANY (ARRAY['E'::text, 'N'::text, 'S'::text])))) AND NOT ((a.class_type = ANY (ARRAY['Vehicles'::text, 'Cyclists'::text])) AND (a.dir = 'EB'::text AND (c.street_main = ANY (ARRAY['Wellington'::text, 'Richmond'::text])) OR a.dir = 'WB'::text AND c.street_main = 'Adelaide'::text))
          GROUP BY a.intersection_uid, c.intersection_name, c.street_main, c.street_cross, a.period_type, a.class_type, a.dir, (a.datetime_bin::date), b.period_name, b.period_range, b.period_id
        )
 SELECT daily.period_type AS aggregation_period,
    intersections.int_id,
    centreline_intersection.intersec5 AS intersection_name,
    traffic_signals.px,
        CASE
            WHEN daily.class_type = 'Buses'::text THEN 'Buses and streetcars'::text
            ELSE daily.class_type
        END AS classification,
    daily.dir,
    daily.period_name,
    round(avg(daily.total_volume), '-1'::integer)::integer AS average_volume
   FROM daily
     JOIN miovision_new.intersections USING (intersection_uid)
     JOIN gis.centreline_intersection USING (int_id)
     JOIN gis.traffic_signals ON traffic_signals.node_id = intersections.int_id
  GROUP BY daily.period_type, intersections.int_id, daily.class_type, centreline_intersection.intersec5, traffic_signals.px, daily.dir, daily.period_name
WITH DATA;