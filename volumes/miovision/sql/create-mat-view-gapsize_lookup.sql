CREATE MATERIALIZED VIEW miovision_api.gapsize_lookup
TABLESPACE pg_default
AS
 WITH temp(period, isodow) AS (
         VALUES ('Weekday'::text,'[1,6)'::int4range), ('Weekend'::text,'[6,8)'::int4range)
	 )
 , mio AS (
         SELECT volumes.intersection_uid,
            datetime_bin(volumes.datetime_bin, 60) AS hourly_bin,
            sum(volumes.volume) AS vol
           FROM miovision_api.volumes
          WHERE volumes.datetime_bin > current_date::timestamp without time zone - interval '60 days'
	 		-- only get data from the past 60 days so that it's always up to date
          GROUP BY volumes.intersection_uid, (datetime_bin(volumes.datetime_bin, 60))
        )
SELECT intersection_uid, 
	 d.period, hourly_bin::time AS time_bin, AVG(vol) AS avg_vol,
	 CASE WHEN AVG(vol) < 100  THEN 20
	 WHEN AVG(vol) >= 100 AND AVG(vol) < 500 THEN 10
	 WHEN AVG(vol) >= 500 AND AVG(vol) < 1500 THEN 5
	 WHEN AVG(vol) > 1500 THEN 2
	 END AS gap_size
	 FROM mio
	 CROSS JOIN temp d
	 WHERE date_part('isodow'::text, hourly_bin)::integer <@ d.isodow
	 GROUP BY intersection_uid, d.period, hourly_bin::time