CREATE OR REPLACE VIEW open_data.volumes_atr_cyclists_permanent AS
SELECT	centreline_id,
	direction,
	location::character varying(65) AS location,
	class_type,
	datetime_bin,
	volume_15min
FROM
(	SELECT 	centreline_id,
		direction,
		location,
		class_type,
		datetime_bin,
		volume_15min,
		dt
	FROM
	(SELECT flow.centreline_id,
	    flow.direction,
	    flow.location,
	    flow.class_type,
	    flow.datetime_bin,
	    flow.volume AS volume_15min,
	    flow.datetime_bin::date AS dt
	   FROM ( SELECT flow_atr.centreline_id,
		    flow_atr.direction,
		    flow_atr.location,
		    flow_atr.class_type,
		    flow_atr.datetime_bin,
		    flow_atr.volume_15min AS volume
		   FROM open_data.flow_atr
		  WHERE flow_atr.station_type = 'Permanent'::text AND flow_atr.volume_15min >= 0::numeric AND flow_atr.class_type = 'Cyclists'::text) flow
	) A
	UNION ALL
	(SELECT centreline_id::bigint,
		direction::text,
		location::text,
		class_type::text,
		datetime_bin::timestamp without time zone,
		volume_15min::int,
        datetime_bin::date AS dt
    FROM 	cycling.cycling_perm_old)
) main
ORDER BY dt DESC, centreline_id, direction;



CREATE OR REPLACE VIEW open_data.volumes_atr_cyclists_shortterm AS
SELECT i.centreline_id,
        CASE
            WHEN i.dir::text = 'EB'::text THEN 'Eastbound'::text
            WHEN i.dir::text = 'WB'::text THEN 'Westbound'::text
            WHEN i.dir::text = 'NB'::text THEN 'Northbound'::text
            WHEN i.dir::text = 'SB'::text THEN 'Southbound'::text
            ELSE NULL::text
        END AS direction,
    i.location,
    'Cyclists'::text AS class_type,
    i.temperature AS daily_temperature,
    i.precipitation AS daily_percipitation,
    ( SELECT ((i.count_date || ' '::text) || d.start_time)::timestamp without time zone AS "timestamp") AS datetime_bin_start,
        CASE
            WHEN d.end_time <> '00:00:00'::time without time zone THEN ( SELECT ((i.count_date || ' '::text) || d.end_time)::timestamp without time zone AS "timestamp")
            ELSE ( SELECT (((i.count_date + '1 day'::interval)::date || ' '::text) || d.end_time)::timestamp without time zone AS "timestamp")
        END AS datetime_bin_end,
    d.volume
   FROM cycling.count_data d
     JOIN cycling.count_info i ON d.count_id = i.count_id
  ORDER BY i.count_date DESC, i.centreline_id, (
        CASE
            WHEN i.dir::text = 'EB'::text THEN 'Eastbound'::text
            WHEN i.dir::text = 'WB'::text THEN 'Westbound'::text
            WHEN i.dir::text = 'NB'::text THEN 'Northbound'::text
            WHEN i.dir::text = 'SB'::text THEN 'Southbound'::text
            ELSE NULL::text
        END), (( SELECT ((i.count_date || ' '::text) || d.start_time)::timestamp without time zone AS "timestamp")) DESC;



DROP MATERIALIZED VIEW IF EXISTS open_data.volumes_atr_vehicles_permanent;


CREATE MATERIALIZED VIEW open_data.volumes_atr_vehicles_permanent AS
SELECT DISTINCT flow.centreline_id,
    flow.direction,
    flow.location,
    flow.class_type,
    flow.datetime_bin,
    flow.volume_15min
   FROM ( SELECT flow_atr.centreline_id,
            flow_atr.direction,
            flow_atr.location,
            flow_atr.class_type,
            flow_atr.datetime_bin,
            flow_atr.volume_15min
           FROM open_data.flow_atr
          WHERE flow_atr.station_type = 'Permanent'::text AND flow_atr.volume_15min >= 0::numeric AND NOT (EXISTS ( SELECT volumes_atr_permanent_exceptions.day,
                    volumes_atr_permanent_exceptions.location,
                    volumes_atr_permanent_exceptions.class_type
                   FROM open_data_staging.volumes_atr_permanent_exceptions
                  WHERE flow_atr.location::text = volumes_atr_permanent_exceptions.location::text AND flow_atr.datetime_bin::date = volumes_atr_permanent_exceptions.day))) flow;



CREATE OR REPLACE VIEW open_data.volumes_atr_vehicles_shortterm AS
SELECT flow_atr.centreline_id,
    flow_atr.direction,
    flow_atr.location,
    flow_atr.class_type,
    flow_atr.datetime_bin,
    flow_atr.volume_15min
   FROM open_data.flow_atr
  WHERE flow_atr.station_type = 'Short Term'::text AND flow_atr.volume_15min >= 0::numeric AND NOT (EXISTS ( SELECT exceptions.datetime_bin,
            exceptions.location
           FROM open_data_staging.volumes_atr_shortterm_exceptions exceptions
          WHERE exceptions.datetime_bin = flow_atr.datetime_bin::date AND exceptions.location::text = flow_atr.location::text));




DROP MATERIALIZED VIEW IF EXISTS open_data.volumes_tmc_all_permanent;

CREATE MATERIALIZED VIEW open_data.volumes_tmc_all_permanent AS
SELECT g.node_id AS int_id,
    c.px,
    c.display_name AS location,
    d.class_type,
        CASE
            WHEN b.movement_alt IS NULL THEN NULL::text
            WHEN a.leg = 'W'::text THEN 'Eastbound'::text
            WHEN a.leg = 'E'::text THEN 'Westbound'::text
            WHEN a.leg = 'S'::text THEN 'Northbound'::text
            WHEN a.leg = 'N'::text THEN 'Southbound'::text
            ELSE NULL::text
        END AS dir_approach,
    a.leg,
    b.movement_alt AS movement,
    a.datetime_bin,
    round(sum(a.volume), 0)::integer AS volume_15min
   FROM miovision_new.volumes_15min_tmc a
     JOIN miovision_new.movements b USING (movement_uid)
     JOIN miovision_new.intersections c USING (intersection_uid)
     JOIN miovision_new.classifications d USING (classification_uid)
     JOIN gis.traffic_signals g USING (px)
  WHERE b.movement_uid <> 4 AND d.classification_uid <> 3
  GROUP BY c.px, c.display_name, d.class_type, a.leg, b.movement_alt, a.datetime_bin, g.node_id
  ORDER BY c.px, a.datetime_bin, d.class_type, a.leg, b.movement_alt;




 DROP MATERIALIZED VIEW IF EXISTS open_data.volumes_tmc_all_shortterm;

CREATE MATERIALIZED VIEW open_data.volumes_tmc_all_shortterm AS
SELECT g.node_id AS int_id,
    "substring"("substring"(a.location::text, 'PX \d{1,5}'::text), '\d{1,5}'::text)::integer AS px,
    a.location,
    d.class_type,
        CASE
            WHEN d.movement IS NULL THEN NULL::text
            WHEN d.leg = 'W'::text THEN 'Eastbound'::text
            WHEN d.leg = 'E'::text THEN 'Westbound'::text
            WHEN d.leg = 'S'::text THEN 'Northbound'::text
            WHEN d.leg = 'N'::text THEN 'Southbound'::text
            ELSE NULL::text
        END AS dir_approach,
    d.leg,
    d.movement,
    b.count_date::date + (c.count_time::time without time zone - '00:15:00'::interval) AS datetime_bin,
    round(avg(d.volume_15min), 0)::integer AS volume_15min
   FROM traffic.arterydata a
     JOIN traffic.countinfomics b USING (arterycode)
     JOIN traffic.det c USING (count_info_id)
     JOIN open_data.flow_tmc_long d USING (id)
     LEFT JOIN gis.traffic_signals g ON g.px = "substring"("substring"(a.location::text, 'PX \d{1,5}'::text), '\d{1,5}'::text)::integer
  WHERE "substring"("substring"(a.location::text, 'PX \d{1,5}'::text), '\d{1,5}'::text) IS NOT NULL
  GROUP BY a.arterycode, a.apprdir, a.location, d.class_type, d.leg, d.movement, b.count_date, c.count_time, g.node_id
 HAVING round(avg(d.volume_15min), 0)::integer > 0
  ORDER BY a.arterycode, b.count_date, c.count_time;




 DROP MATERIALIZED VIEW IF EXISTS open_data.volumes_tmc_permanent_bikes;

CREATE MATERIALIZED VIEW open_data.volumes_tmc_permanent_bikes AS
 SELECT g.node_id AS int_id,
    c.px,
    c.intersection_name AS location,
    d.class_type,
    b.movement_alt AS movement,
    a.leg,
    a.datetime_bin,
    round(sum(a.volume), 0)::integer AS volume_15min
   FROM miovision_new.volumes_15min_tmc a
     JOIN miovision_new.movements b USING (movement_uid)
     JOIN miovision_new.intersections c USING (intersection_uid)
     JOIN miovision_new.classifications d USING (classification_uid)
     LEFT JOIN gis.traffic_signals g ON g.px = c.px
  WHERE b.movement_uid <> 4 AND d.classification_uid <> 3 AND d.class_type = 'Cyclists'::text
  GROUP BY c.px, c.intersection_name, d.class_type, a.leg, b.movement_alt, a.datetime_bin, g.node_id
  ORDER BY c.px, a.datetime_bin, d.class_type, a.leg, b.movement_alt;





DROP MATERIALIZED VIEW IF EXISTS open_data_staging.volumes_atr_bikes_exceptions;

CREATE MATERIALIZED VIEW open_data_staging.volumes_atr_bikes_exceptions AS
 SELECT volumes_atr_bikes.index,
    volumes_atr_bikes.date,
    volumes_atr_bikes.centreline_id,
    volumes_atr_bikes.direction
   FROM volumes_atr_bikes
WITH DATA;



DROP MATERIALIZED VIEW IF EXISTS open_data_staging.volumes_atr_permanent_day_hour_location;

CREATE MATERIALIZED VIEW open_data_staging.volumes_atr_permanent_day_hour_location AS
 SELECT volumes_atr_permanent.datetime_bin::date AS day,
    date_part('hour'::text, volumes_atr_permanent.datetime_bin) AS hour,
    volumes_atr_permanent.location,
    volumes_atr_permanent.class_type,
    max(volumes_atr_permanent.volume_15min) AS max,
    min(volumes_atr_permanent.volume_15min) AS min,
    avg(volumes_atr_permanent.volume_15min) AS avg,
    round(percentile_cont(0.50::double precision) WITHIN GROUP (ORDER BY (volumes_atr_permanent.volume_15min::double precision))::numeric, 2) AS median
   FROM ( SELECT DISTINCT flow.centreline_id,
            flow.direction,
            flow.location,
            flow.class_type,
            flow.datetime_bin,
            flow.volume_15min
           FROM ( SELECT flow_atr.centreline_id,
                    flow_atr.direction,
                    flow_atr.location,
                    flow_atr.class_type,
                    flow_atr.datetime_bin,
                    flow_atr.volume_15min
                   FROM open_data.flow_atr
                  WHERE flow_atr.station_type = 'Permanent'::text AND flow_atr.volume_15min >= 0::numeric) flow
  ORDER BY 1, 5, 2) volumes_atr_permanent
  GROUP BY (volumes_atr_permanent.datetime_bin::date), (date_part('hour'::text, volumes_atr_permanent.datetime_bin)), volumes_atr_permanent.location, volumes_atr_permanent.class_type
WITH DATA;




DROP MATERIALIZED VIEW IF EXISTS open_data_staging.volumes_atr_permanent_exceptions_2;

CREATE MATERIALIZED VIEW open_data_staging.volumes_atr_permanent_exceptions_2 AS
 SELECT DISTINCT greater_5.direction,
    greater_5.centreline_id,
    greater_5.class_type,
    greater_5.dt,
    p.location
   FROM ( SELECT greater_than_5.dt,
            count(*) AS num_locations_with_at_least_5_outliers_this_day
           FROM ( SELECT atr_permanent_outliers.centreline_id,
                    atr_permanent_outliers.direction,
                    atr_permanent_outliers.class_type,
                    atr_permanent_outliers.datetime_bin::date AS dt,
                    count(*) AS cnt
                   FROM atr_permanent_outliers
                  WHERE atr_permanent_outliers.centreline_id IS NOT NULL
                  GROUP BY atr_permanent_outliers.centreline_id, atr_permanent_outliers.direction, atr_permanent_outliers.class_type, (atr_permanent_outliers.datetime_bin::date)
                 HAVING count(*) > 5) greater_than_5
          GROUP BY greater_than_5.dt
         HAVING count(*) <= 3
          ORDER BY (count(*))) dates
     JOIN ( SELECT atr_permanent_outliers.centreline_id,
            atr_permanent_outliers.direction,
            atr_permanent_outliers.class_type,
            atr_permanent_outliers.datetime_bin::date AS dt,
            count(*) AS cnt
           FROM atr_permanent_outliers
          WHERE atr_permanent_outliers.centreline_id IS NOT NULL
          GROUP BY atr_permanent_outliers.centreline_id, atr_permanent_outliers.direction, atr_permanent_outliers.class_type, (atr_permanent_outliers.datetime_bin::date)
         HAVING count(*) > 5) greater_5 ON dates.dt = greater_5.dt
     JOIN open_data.volumes_atr_vehicles_permanent p ON greater_5.dt = p.datetime_bin::date AND greater_5.centreline_id = p.centreline_id AND greater_5.direction::text = p.direction::text AND greater_5.class_type = p.class_type
  ORDER BY greater_5.dt
WITH DATA;



DROP MATERIALIZED VIEW IF EXISTS open_data_staging.volumes_atr_shortterm_exceptions;

CREATE MATERIALIZED VIEW open_data_staging.volumes_atr_shortterm_exceptions AS
 SELECT o1.datetime_bin::date AS datetime_bin,
    o1.location
   FROM open_data.volumes_atr_vehicles_shortterm o1
     JOIN open_data.volumes_atr_vehicles_shortterm o2 ON o1.location::text = o2.location::text
  WHERE
        CASE
            WHEN (o1.datetime_bin - o2.datetime_bin) < '00:00:00'::interval THEN - (o1.datetime_bin - o2.datetime_bin)
            ELSE o1.datetime_bin - o2.datetime_bin
        END <= '01:00:00'::interval AND (o1.volume_15min > 20::numeric AND o2.volume_15min = 0::numeric OR o1.volume_15min < 450::numeric AND o2.volume_15min > 1000::numeric)
UNION
 SELECT o2.datetime_bin::date AS datetime_bin,
    o2.location
   FROM open_data.volumes_atr_vehicles_shortterm o1
     JOIN open_data.volumes_atr_vehicles_shortterm o2 ON o1.location::text = o2.location::text
  WHERE
        CASE
            WHEN (o1.datetime_bin - o2.datetime_bin) < '00:00:00'::interval THEN - (o1.datetime_bin - o2.datetime_bin)
            ELSE o1.datetime_bin - o2.datetime_bin
        END <= '01:00:00'::interval AND (o1.volume_15min > 20::numeric AND o2.volume_15min = 0::numeric OR o1.volume_15min < 450::numeric AND o2.volume_15min > 1000::numeric)
UNION
 SELECT v.datetime_bin::date AS datetime_bin,
    v.location
   FROM open_data.volumes_atr_vehicles_shortterm v
  GROUP BY (v.datetime_bin::date), v.location
 HAVING count(*) <= 3 AND (max(v.volume_15min) > 1000::numeric OR min(v.volume_15min) <= 5::numeric) OR avg(v.volume_15min) = 0::numeric
WITH DATA;



DROP VIEW open_data_staging.volumes_atr_permanent_exceptions;

CREATE OR REPLACE VIEW open_data_staging.volumes_atr_permanent_exceptions AS
 SELECT DISTINCT volumes_atr_permanent_day_hour_location.day,
    volumes_atr_permanent_day_hour_location.location,
    volumes_atr_permanent_day_hour_location.class_type
   FROM open_data_staging.volumes_atr_permanent_day_hour_location
  WHERE volumes_atr_permanent_day_hour_location.class_type = 'Vehicles'::text AND (volumes_atr_permanent_day_hour_location.max > (4::numeric * volumes_atr_permanent_day_hour_location.median) AND volumes_atr_permanent_day_hour_location.max > 1000::numeric OR volumes_atr_permanent_day_hour_location.max = volumes_atr_permanent_day_hour_location.min OR volumes_atr_permanent_day_hour_location.max > (10::numeric * volumes_atr_permanent_day_hour_location.min) AND volumes_atr_permanent_day_hour_location.max > 1000::numeric);
