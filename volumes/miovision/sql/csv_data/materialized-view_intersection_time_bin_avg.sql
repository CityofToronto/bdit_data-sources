CREATE MATERIALIZED VIEW miovision.int_time_bin_avg AS(
    SELECT volumes_15min_by_class.intersection_uid,
            volumes_15min_by_class.class_type_id,
            volumes_15min_by_class.dir,
            volumes_15min_by_class.leg,
            volumes_15min_by_class.period_type,
            volumes_15min_by_class.datetime_bin::time without time zone AS time_bin,
            avg(volumes_15min_by_class.total_volume) AS avg_volume
           FROM miovision_new.volumes_15min_by_class
          GROUP BY volumes_15min_by_class.intersection_uid, volumes_15min_by_class.class_type_id, volumes_15min_by_class.period_type,
                   volumes_15min_by_class.dir, volumes_15min_by_class.leg, time_bin
);
GRANT SELECT ON TABLE miovision.int_time_bin_avg TO bdit_humans;
COMMENT ON MATERIALIZED VIEW miovision.int_time_bin_avg IS 'Average ATR volume by 15-minute bin';
CREATE INDEX ON miovision.int_time_bin_avg(intersection_uid, class_type_id, dir, leg, period_type);
ANALYZE miovision.int_time_bin_avg;