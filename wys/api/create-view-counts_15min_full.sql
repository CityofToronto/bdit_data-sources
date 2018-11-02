-- DROP MATERIALIZED VIEW wys.counts_15min_full;

CREATE MATERIALIZED VIEW wys.counts_15min_full AS 
 WITH valid_bins AS (
         SELECT DISTINCT a_1.api_id,
            a_1.datetime_bin,
            b.speed_id
           FROM wys.counts_15min a_1
             CROSS JOIN wys.speed_bins b
        )
 SELECT a.api_id,
    a.datetime_bin,
    a.speed_id,
    COALESCE(c.count, 0) AS count
   FROM valid_bins a
     LEFT JOIN ( SELECT counts_15min.counts_15min,
            counts_15min.api_id,
            counts_15min.datetime_bin,
            counts_15min.speed_id,
            counts_15min.count,
            counts_15min.volumes_15min_uid
           FROM wys.counts_15min) c USING (api_id, datetime_bin, speed_id)
  ORDER BY a.api_id, a.datetime_bin, a.speed_id
WITH DATA;