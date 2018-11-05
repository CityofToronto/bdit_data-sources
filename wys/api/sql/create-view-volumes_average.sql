--DROP MATERIALIZED VIEW wys.volume_average;

CREATE MATERIALIZED VIEW wys.volume_average AS 
 WITH valid_bins AS (
         SELECT b_1.tm::time without time zone AS time_bin,
            a_1.api_id,
            a_1.period,
            c.dow
           FROM wys.report_dates a_1
             CROSS JOIN generate_series('2017-01-01 06:00:00'::timestamp without time zone, '2017-01-01 21:45:00'::timestamp without time zone, '00:15:00'::interval) b_1(tm)
             CROSS JOIN generate_series(0, 6, 1) c(dow)
        )
 SELECT b.api_id,
    b.period,
    b.dow,
    b.time_bin,
    avg(a.count) AS count
   FROM valid_bins b
     LEFT JOIN wys.volumes_15min a ON a.api_id = b.api_id AND to_char(date_trunc('month'::text, a.datetime_bin), 'Mon YYYY'::text) = b.period AND a.datetime_bin::time without time zone = b.time_bin AND date_part('dow'::text, a.datetime_bin) = b.dow::double precision
  GROUP BY b.api_id, b.period, b.dow, b.time_bin
WITH DATA;