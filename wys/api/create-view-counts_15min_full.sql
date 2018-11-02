-- DROP MATERIALIZED VIEW wys.counts_15min_full;

CREATE MATERIALIZED VIEW wys.counts_15min_full AS 
 WITH valid_bins AS (
         SELECT a_1.dt + b_1.tm::time without time zone AS datetime_bin,
            a_1.api_id,
            a_1.period,
            c_1.speed_id
           FROM wys.report_dates a_1
             CROSS JOIN generate_series('2017-01-01 06:00:00'::timestamp without time zone, '2017-01-01 21:45:00'::timestamp without time zone, '00:15:00'::interval) b_1(tm)
             CROSS JOIN generate_series(1, 17, 1) c_1(speed_id)
        )
 SELECT c.api_id,
    c.datetime_bin,
    c.speed_id,
    COALESCE(d.count::numeric, e.count) AS count
   FROM valid_bins c
     LEFT JOIN wys.counts_15min d USING (api_id, datetime_bin, speed_id)
     LEFT JOIN wys.counts_average e ON c.api_id = e.api_id AND c.datetime_bin::time without time zone = e.time_bin AND to_char(date_trunc('month'::text, c.datetime_bin), 'Mon YYYY'::text) = e.period AND date_part('dow'::text, c.datetime_bin) = e.dow::double precision AND e.speed_id = c.speed_id
WITH DATA;