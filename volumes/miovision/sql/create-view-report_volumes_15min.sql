CREATE MATERIALIZED VIEW miovision_new.report_volumes_15min AS 
 WITH valid_bins AS (
         SELECT a_1.intersection_uid,
            a_1.class_type,
            a_1.dt + b_1.b::time without time zone AS datetime_bin,
            a_1.period_type
           FROM miovision_new.report_dates a_1
             CROSS JOIN generate_series('2017-01-01 06:00:00'::timestamp without time zone, '2017-01-01 19:45:00'::timestamp without time zone, '00:15:00'::interval) b_1(b)
             LEFT JOIN miovision.exceptions c ON a_1.intersection_uid = c.intersection_uid AND a_1.class_type = c.class_type AND (a_1.dt + b_1.b) <@ c.excluded_datetime
          WHERE c.excluded_datetime IS NULL
          ORDER BY a_1.intersection_uid, a_1.class_type, (a_1.dt + b_1.b::time without time zone)
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
        )
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
WITH DATA;

ALTER TABLE miovision_new.report_volumes_15min
  OWNER TO bdit_humans;