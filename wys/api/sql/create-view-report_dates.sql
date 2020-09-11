-- DROP MATERIALIZED VIEW wys.report_dates;

CREATE MATERIALIZED VIEW wys.report_dates AS 
 SELECT volumes_15min.api_id,
    to_char(date_trunc('month'::text, volumes_15min.datetime_bin::date::timestamp with time zone), 'Mon YYYY'::text) AS period,
    volumes_15min.datetime_bin::date AS dt,
    date_part('dow'::text, volumes_15min.datetime_bin::date) AS dow
   FROM wys.volumes_15min
  WHERE volumes_15min.datetime_bin::time without time zone >= '06:00:00'::time without time zone AND volumes_15min.datetime_bin::time without time zone < '21:45:00'::time without time zone
  GROUP BY volumes_15min.api_id, (volumes_15min.datetime_bin::date)
 HAVING count(DISTINCT volumes_15min.datetime_bin::time without time zone) >= 40
WITH DATA;
