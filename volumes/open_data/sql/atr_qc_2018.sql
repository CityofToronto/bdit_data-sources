CREATE MATERIALIZED VIEW open_data.flow_count_type AS 
 WITH artery_days AS (
         SELECT countinfo.arterycode,
            arterydata.count_type,
            count(1) AS days
           FROM traffic.countinfo
             JOIN traffic.arterydata USING (arterycode)
          GROUP BY countinfo.arterycode, arterydata.count_type
          ORDER BY countinfo.arterycode
        )
 SELECT artery_days.arterycode,
        CASE
            WHEN artery_days.count_type::text = 'PermAutom'::text AND artery_days.days > 40 OR artery_days.days > 100 THEN 'Permanent'::text
            ELSE 'Short Term'::text
        END AS station_type
   FROM artery_days
  WHERE artery_days.arterycode <> ALL (ARRAY[39053::bigint, 39054::bigint, 39055::bigint, 39056::bigint])
WITH DATA;

CREATE MATERIALIZED VIEW open_data.flow_atr AS 
 SELECT a.arterycode,
    d.station_type,
    b.centreline_id,
        CASE
            WHEN a.apprdir::text = 'EB'::text THEN 'Eastbound'::character varying
            ELSE a.apprdir
        END AS direction,
    a.location,
        CASE
            WHEN c.category_id = 7 THEN 'Cyclists'::text
            ELSE 'Vehicles'::text
        END AS class_type,
    c.count_date + e.timecount::time without time zone AS datetime_bin,
    sum(e.count) AS volume_15min
   FROM traffic.arterydata a
     JOIN prj_volume.artery_tcl b USING (arterycode)
     JOIN traffic.countinfo c USING (arterycode)
     JOIN open_data.flow_count_type d USING (arterycode)
     JOIN traffic.cnt_det e USING (count_info_id)
  WHERE a.apprdir::text <> ''::text
  GROUP BY a.arterycode, c.category_id, d.station_type, b.centreline_id, a.apprdir, a.location, c.count_date, e.timecount
  ORDER BY a.arterycode, e.timecount
WITH DATA;

DROP VIEW IF EXISTS open_data.volumes_atr_bikes; 


CREATE OR REPLACE VIEW open_data.volumes_atr_bikes AS 
 SELECT flow.centreline_id,
    flow.direction,
    flow.location,
    flow.class_type,
    flow.datetime_bin,
    flow.volume AS volume_15min
   FROM ( SELECT flow_atr.centreline_id,
            flow_atr.direction,
            flow_atr.location,
            flow_atr.class_type,
            flow_atr.datetime_bin,
            flow_atr.volume_15min AS volume
           FROM open_data.flow_atr
          WHERE flow_atr.station_type = 'Permanent'::text AND flow_atr.volume_15min >= 0 AND flow_atr.class_type = 'Cyclists') flow
UNION ALL
 SELECT a.centreline_id,
    a.direction,
    a.location_desc AS location,
    'Cyclists'::text AS class_type,
    b.datetime_bin,
    b.volume_15min AS volume_15min
   FROM ecocounter.sites a
     JOIN ecocounter.volumes b USING (site_id)
     WHERE b.volume_15min >= 0
  ORDER BY 1, 5, 2;


-- check for duplicates
-- reutrns nothing so there are no duplicates 
SELECT centreline_id, direction, location, datetime_bin, class_type, COUNT(*) 
FROM open_data.volumes_atr_bikes
GROUP BY centreline_id, direction, location, datetime_bin, class_type
HAVING COUNT(*) > 1;


-- spatial check to ensure bike station locations are correct 
SELECT DISTINCT location, centreline_id, lf_name, geom 
INTO crosic.atr_bike_locations
FROM open_data.volumes_atr_bikes o JOIN gis.centreline_hist ON centreline_id = geo_id;


SELECT 
(SELECT COUNT(*) FROM open_data.volumes_atr_bikes) count_bikes,
(SELECT COUNT(*) FROM (
 SELECT flow.centreline_id,
    flow.direction,
    flow.location,
    flow.class_type,
    flow.datetime_bin,
    flow.resolution,
    flow.volume
   FROM ( SELECT flow_atr.centreline_id,
            flow_atr.direction,
            flow_atr.location,
            flow_atr.class_type,
            flow_atr.datetime_bin,
            '15 minutes' AS resolution,
            flow_atr.volume_15min AS volume
           FROM open_data.flow_atr
          WHERE flow_atr.station_type = 'Permanent'::text AND flow_atr.class_type = 'Cyclists' AND flow_atr.volume_15min >= 0) flow
UNION ALL
 SELECT a.centreline_id,
    a.direction,
    a.location_desc AS location,
    'Cyclists'::text AS class_type,
    b.datetime_bin,
    '15 minutes'::text AS resolution,
    b.volume_15min AS volume
   FROM ecocounter.sites a
     JOIN ecocounter.volumes b USING (site_id)
     WHERE b.volume_15min >= 0
  ORDER BY 1, 5, 2) x  ) count_original;



-- ATR SHORT-TERM

DROP VIEW IF EXISTS open_data.volumes_atr_shortterm; 

CREATE OR REPLACE VIEW open_data.volumes_atr_shortterm AS 
SELECT flow_atr.centreline_id,
            flow_atr.direction,
            flow_atr.location,
            flow_atr.class_type,
            flow_atr.datetime_bin,
            flow_atr.volume_15min AS volume_15min
           FROM open_data.flow_atr
          WHERE flow_atr.station_type = 'Short Term'::text and flow_atr.volume_15min >= 0;



-- check for duplicates
-- reutrns nothing now so there are no duplicates 
-- there were duplicates before, which is why I added a DISTINCT clause in the query that creates the view
SELECT centreline_id, direction, location, datetime_bin, class_type, volume_15min, COUNT(*) 
FROM open_data.volumes_atr_shortterm
GROUP BY centreline_id, direction, location, datetime_bin, class_type, volume_15min 
HAVING COUNT(*) > 1;


-- check to make sure number of records is identical to source table
-- it is not because duplicates were removed
SELECT 
(SELECT COUNT(*) FROM open_data.volumes_atr_shortterm) count_shortterm,
(SELECT COUNT(*) FROM (
CREATE OR REPLACE VIEW vz_challenge.volumes_atr_shortterm AS 
 SELECT flow.centreline_id,
    flow.direction,
    flow.location,
    flow.class_type,
    flow.datetime_bin,
    flow.resolution,
    flow.volume
   FROM ( SELECT flow_atr.centreline_id,
            flow_atr.direction,
            flow_atr.location,
            flow_atr.class_type,
            flow_atr.datetime_bin,
            '15 minutes' AS resolution,
            flow_atr.volume_15min AS volume
           FROM open_data.flow_atr
          WHERE flow_atr.station_type = 'Short Term'::text flow_atr.volume_15min >= 0) flow) ) count_original;


-- ATR Permanent

DROP VIEW IF EXISTS open_data.volumes_atr_permanent; 

CREATE OR REPLACE VIEW open_data.volumes_atr_permanent AS 
 SELECT DISTINCT flow.centreline_id,
    flow.direction,
    flow.location,
    flow.class_type,
    flow.datetime_bin,
    flow.volume_15min volume_15min
   FROM ( SELECT flow_atr.centreline_id,
            flow_atr.direction,
            flow_atr.location,
            flow_atr.class_type,
            flow_atr.datetime_bin,
            flow_atr.volume_15min AS volume_15min
           FROM open_data.flow_atr
          WHERE flow_atr.station_type = 'Permanent'::text AND volume_15min >= 0) flow
UNION ALL
 SELECT a.centreline_id,
    a.direction,
    a.location_desc AS location,
    'Cyclists'::text AS class_type,
    b.datetime_bin,
    b.volume_15min AS volume_15min
   FROM ecocounter.sites a
     JOIN ecocounter.volumes b USING (site_id)
   WHERE volume_15min >= 0
  ORDER BY 1, 5, 2;

-- check for duplicates

SELECT centreline_id, direction, location, datetime_bin, class_type, volume_15min, COUNT(*) 
FROM open_data.volumes_atr_permanent
GROUP BY centreline_id, direction, location, datetime_bin, class_type, volume_15min 
HAVING COUNT(*) > 1
LIMIT 20;


-- check to make sure number of records is identical to source table
SELECT 
(SELECT COUNT(*) FROM open_data.volumes_atr_permanent) count_permanent,
(SELECT COUNT(*) FROM 
(SELECT DISTINCT ON (flow.centreline_id,
    flow.direction,
    flow.location,
    flow.class_type,
    flow.datetime_bin,
    flow.volume)
    flow.centreline_id,
    flow.direction,
    flow.location,
    flow.class_type,
    flow.datetime_bin,
    flow.resolution,
    flow.volume
   FROM ( SELECT flow_atr.centreline_id,
            flow_atr.direction,
            flow_atr.location,
            flow_atr.class_type,
            flow_atr.datetime_bin,
            '15 minutes' AS resolution,
            flow_atr.volume_15min AS volume
           FROM open_data.flow_atr
          WHERE flow_atr.station_type = 'Permanent'::text and volume_15min >= 0) flow
UNION ALL
 SELECT a.centreline_id,
    a.direction,
    a.location_desc AS location,
    'Cyclists'::text AS class_type,
    b.datetime_bin,
    '15 minutes'::text AS resolution,
    b.volume_15min AS volume
   FROM ecocounter.sites a
     JOIN ecocounter.volumes b USING (site_id)
     WHERE b.volume_15min >= 0
  ORDER BY 1, 5, 2) x  
  ) count_original;

