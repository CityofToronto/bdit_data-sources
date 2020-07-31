CREATE MATERIALIZED VIEW miovision_api.gapsize_lookup
TABLESPACE pg_default
AS
 WITH temp(period, isodow) AS (
         VALUES ('Weekday'::text,'[1,6)'::int4range), ('Weekend'::text,'[6,8)'::int4range)
        ), mio AS (
         SELECT volumes.intersection_uid,
            datetime_bin(volumes.datetime_bin, 60) AS hourly_bin,
            sum(volumes.volume) AS vol
           FROM miovision_api.volumes
          WHERE volumes.datetime_bin > ('now'::text::date::timestamp without time zone - '60 days'::interval)
          GROUP BY volumes.intersection_uid, (datetime_bin(volumes.datetime_bin, 60))
        )
 SELECT mio.intersection_uid,
    d.period,
    mio.hourly_bin::time without time zone AS time_bin,
    avg(mio.vol) AS avg_vol,
        CASE
            WHEN avg(mio.vol) < 100::numeric THEN 20
            WHEN avg(mio.vol) >= 100::numeric AND avg(mio.vol) < 500::numeric THEN 15
            WHEN avg(mio.vol) >= 500::numeric AND avg(mio.vol) < 1500::numeric THEN 10
            WHEN avg(mio.vol) > 1500::numeric THEN 5
            ELSE NULL::integer
        END AS gap_size
   FROM mio
     CROSS JOIN temp d
  WHERE date_part('isodow'::text, mio.hourly_bin)::integer <@ d.isodow
  GROUP BY mio.intersection_uid, d.period, (mio.hourly_bin::time without time zone)
WITH DATA;