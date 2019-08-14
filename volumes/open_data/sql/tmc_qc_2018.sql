/*
DROP VIEW IF EXISTS crosic.px_int_id; 
DROP VIEW IF EXISTS crosic.get_int_id;


CREATE VIEW crosic.get_int_id AS (
SELECT intersections.objectid oid, px, (SELECT int_id FROM gis.centreline_intersection WHERE objectid = intersections.objectid) AS int_id, location, main, side_1
FROM 
-- exclude certain objectid 's because these objectid's are made up of more than 2 streets, and 2 of the streets have very similar names
-- i.e. King St E / Yonge St / King St W  
-- this intersection is not supposed to get matched to anything
--  but was being matched to streets such as King and Bay since King St W matches King and King St E matches King 
-- so there would be 2 matches for this intersection 
-- fixed this issue by excluding intersections with two streets that have a levenshtein distance of 1 or 2
(SELECT * FROM gis.centreline_intersection_streets
)
AS intersections,
(SELECT DISTINCT px, location, (SELECT side_1 FROM gis.traffic_signals WHERE px = t.px) side_1, (SELECT main FROM gis.traffic_signals WHERE px = t.px) main  
FROM vz_challenge.volumes_tmc_permanent t) tmc


WHERE ((levenshtein(TRIM(LOWER(intersections.street)), (SELECT LOWER(side_1) FROM gis.traffic_signals WHERE px = tmc.px), 1, 1, 1) < 3 OR
levenshtein(TRIM(LOWER(intersections.street)),(SELECT LOWER(main) FROM gis.traffic_signals WHERE px = tmc.px), 1, 1, 1) < 3))



GROUP BY intersections.objectid, tmc.location, tmc.px, main, side_1
HAVING COUNT(intersections.street) > 1
ORDER BY AVG(LEAST(levenshtein(LOWER(intersections.street), LOWER(side_1), 1, 1, 2), 
levenshtein(LOWER(intersections.street), LOWER(main), 1, 1, 2))) 

) ;

CREATE VIEW crosic.px_int_id AS (
SELECT DISTINCT ON (c.px) c.px, c.location, main, side_1, int_id
FROM (SELECT DISTINCT px, location, class_type FROM vz_challenge.volumes_tmc_permanent) tmc JOIN crosic.get_int_id c ON tmc.px = c.px
WHERE class_type = 'Cyclists' 
AND oid NOT IN 
(SELECT g1.objectid 
FROM gis.centreline_intersection_streets g1, gis.centreline_intersection_streets g2, gis.centreline_intersection_streets g3
WHERE g1.objectid = g2.objectid AND g1.objectid = g3.objectid AND
TRIM(g1.street) <> TRIM(g2.street) AND TRIM(g1.street) <> TRIM(g3.street) AND TRIM(g2.street) <> TRIM(g3.street) 
AND (levenshtein(TRIM(g1.street), TRIM(g2.street)) < 3 OR levenshtein(TRIM(g1.street), TRIM(g3.street)) < 3 OR levenshtein(TRIM(g2.street), TRIM(g3.street)) < 3) 

AND ( (levenshtein(LOWER(main), TRIM(LOWER(g1.street))) >= 3 AND levenshtein(LOWER(main), TRIM(LOWER(g2.street))) >= 3 AND levenshtein(LOWER(main), TRIM(LOWER(g3.street))) >= 3) OR  
(levenshtein(LOWER(side_1), TRIM(LOWER(g1.street))) >= 3 AND levenshtein(LOWER(side_1), TRIM(LOWER(g2.street))) >= 3 AND levenshtein(LOWER(side_1), TRIM(LOWER(g3.street))) >= 3) ) 
)
);






-- QC -----


-- look at final view, with names of int_id's from centreline_intersection file
SELECT px, c.int_id, objectid, location, main, side_1, intersec5
FROM crosic.px_int_id c LEFT JOIN gis.centreline_intersection g ON c.int_id = g.int_id;





-- find whats not in the final view
-- 'king st e / yonge / king st w' intersection is not in final view 
SELECT DISTINCT px, location, (SELECT side_1 FROM gis.traffic_signals WHERE px = t.px) side_1, (SELECT main FROM gis.traffic_signals WHERE px = t.px) main
FROM vz_challenge.volumes_tmc_permanent t
WHERE t.px NOT IN (SELECT px FROM crosic.px_int_id) and class_type = 'Cyclists';


SELECT * FROM  gis.centreline_intersection_streets g1, gis.centreline_intersection_streets g2, gis.centreline_intersection_streets g3 WHERE g1.objectid = 20352 AND g2.objectid = 20352 AND g3.objectid = 20352 
AND
TRIM(g1.street) <> TRIM(g2.street) AND TRIM(g1.street) <> TRIM(g3.street) AND TRIM(g2.street) <> TRIM(g3.street) 
AND (levenshtein(TRIM(g1.street), TRIM(g2.street)) < 3 OR levenshtein(TRIM(g1.street), TRIM(g3.street)) < 3 OR levenshtein(TRIM(g2.street), TRIM(g3.street)) < 3) 
AND ( (levenshtein(LOWER('KING ST'), TRIM(LOWER(g1.street))) >= 3 AND levenshtein(LOWER('KING ST'), TRIM(LOWER(g2.street))) >= 3 AND levenshtein(LOWER('KING ST'), TRIM(LOWER(g3.street))) >= 3) OR  
(levenshtein(LOWER('YONGE ST'), TRIM(LOWER(g1.street))) >= 3 AND levenshtein(LOWER('YONGE ST'), TRIM(LOWER(g2.street))) >= 3 AND levenshtein(LOWER('YONGE ST'), TRIM(LOWER(g3.street))) >= 3) ) 



-- try to imporve by getting rid of the crosic.get_int_id intermediate view 


*/

-- TMC PERMANENT

DROP MATERIALIZED VIEW IF EXISTS open_data.volumes_tmc_permanent; 

CREATE MATERIALIZED VIEW open_data.volumes_tmc_permanent AS 
 SELECT 
    g.node_id AS int_id,
    c.px,
    c.intersection_name AS location,
    d.class_type,
    a.leg,
    b.movement_alt AS movement,
    a.datetime_bin,
    round(sum(a.volume), 0)::integer AS volume_15min
   FROM
     miovision.volumes_15min_tmc a
     JOIN miovision.movements b USING (movement_uid)
     JOIN miovision.intersections c USING (intersection_uid)
     JOIN miovision.classifications d USING (classification_uid)  
     JOIN gis.traffic_signals g USING (px)
  WHERE b.movement_uid <> 4 AND d.classification_uid <> 3
  GROUP BY c.px, c.intersection_name, d.class_type, a.leg, b.movement_alt, a.datetime_bin, g.node_id
  ORDER BY c.px, a.datetime_bin, d.class_type, a.leg, b.movement_alt
WITH DATA;


-- QC

-- check for duplicates
-- reutrns nothing so there are no duplicates 
SELECT int_id, px, datetime_bin, leg, movement, class_type, COUNT(*) 
FROM open_data.volumes_tmc_permanent
GROUP BY int_id, px, datetime_bin, leg, movement, class_type
HAVING COUNT(*) > 1;


-- check to make sure number of records is identical to source table
SELECT 
(SELECT COUNT(*) FROM open_data.volumes_tmc_permanent) count_perm,
(SELECT COUNT(*) FROM 
-- this is the query that created the vz_challenge.volumes_tmc_permanent table
(SELECT c.px,
    'Permanent' AS station_type,
    c.intersection_name AS location,
    d.class_type,
    a.leg,
    b.movement_alt AS movement,
    a.datetime_bin,
    round(sum(a.volume), 0)::integer AS volume_15min
   FROM miovision.volumes_15min_tmc a
     JOIN miovision.movements b USING (movement_uid)
     JOIN miovision.intersections c USING (intersection_uid)
     JOIN miovision.classifications d USING (classification_uid)
  WHERE b.movement_uid <> 4 AND d.classification_uid <> 3
  GROUP BY c.px, c.intersection_name, d.class_type, a.leg, b.movement_alt, a.datetime_bin
  ORDER BY c.px, a.datetime_bin, d.class_type, a.leg, b.movement_alt) x) count_original;


-- check int_id matching is correct 
-- note King / Peter and King / Blue Jays Way are the same intersection (verified by Google Maps)
SELECT DISTINCT node_id int_id, t.px px, t.location, g.main || ' / ' || g.side_1 AS intersection_traffic_signal -- , (SELECT geom from gis.centreline_intersection where int_id = g.node_id) geom
FROM vz_challenge.volumes_tmc_permanent t LEFT JOIN gis.traffic_signals g ON g.px = t.px; 




-- TMC BIKES 

DROP VIEW IF EXISTS open_data.volumes_tmc_bikes; 


DROP MATERIALIZED VIEW IF EXISTS open_data.volumes_tmc_bikes; 

CREATE MATERIALIZED VIEW open_data.volumes_tmc_bikes AS 
 SELECT 
    g.node_id AS int_id,
    c.px,
    c.intersection_name AS location,
    d.class_type,
    a.leg,
    b.movement_alt AS movement,
    a.datetime_bin,
    round(sum(a.volume), 0)::integer AS volume_15min
   FROM miovision.volumes_15min_tmc a
     JOIN miovision.movements b USING (movement_uid)
     JOIN miovision.intersections c USING (intersection_uid)
     JOIN miovision.classifications d USING (classification_uid)
     LEFT JOIN gis.traffic_signals g ON g.px = c.px
  WHERE b.movement_uid <> 4 AND d.classification_uid <> 3 AND class_type = 'Cyclists'
  GROUP BY c.px, c.intersection_name, d.class_type, a.leg, b.movement_alt, a.datetime_bin, g.node_id
  ORDER BY c.px, a.datetime_bin, d.class_type, a.leg, b.movement_alt
WITH DATA;


-- QC

-- check for duplicates
-- reutrns nothing so there are no duplicates 
SELECT int_id, px, datetime_bin, leg, movement, COUNT(*) 
FROM open_data.volumes_tmc_bikes
GROUP BY int_id, px, datetime_bin, leg, movement
HAVING COUNT(*) > 1


-- check to make sure number of records is identical to source table
SELECT 
(SELECT COUNT(*) FROM open_data.volumes_tmc_bikes) count_bikes,
(SELECT COUNT(*) FROM 
-- this is the query that created the vz_challenge.volumes_tmc_permanent table
(SELECT c.px,
    'Permanent' AS station_type,
    c.intersection_name AS location,
    d.class_type,
    a.leg,
    b.movement_alt AS movement,
    a.datetime_bin,
    round(sum(a.volume), 0)::integer AS volume_15min
   FROM miovision.volumes_15min_tmc a
     JOIN miovision.movements b USING (movement_uid)
     JOIN miovision.intersections c USING (intersection_uid)
     JOIN miovision.classifications d USING (classification_uid)
  WHERE b.movement_uid <> 4 AND d.classification_uid <> 3
  GROUP BY c.px, c.intersection_name, d.class_type, a.leg, b.movement_alt, a.datetime_bin
  ORDER BY c.px, a.datetime_bin, d.class_type, a.leg, b.movement_alt) x WHERE class_type = 'Cyclists') count_original;


-- check int_id matching is correct 
-- note King / Peter and King / Blue Jays Way are the same intersection (verified by Google Maps)
SELECT DISTINCT node_id int_id, t.px px, t.location, g.main || ' / ' || g.side_1 AS intersection_traffic_signal -- , (SELECT geom from gis.centreline_intersection where int_id = g.node_id) geom
FROM vz_challenge.volumes_tmc_permanent t LEFT JOIN gis.traffic_signals g ON g.px = t.px
WHERE class_type = 'Cyclists'; 




CREATE MATERIALIZED VIEW open_data.flow_tmc_long AS 
 SELECT r.id::bigint AS id,
        CASE
            WHEN (r.k).key ~ 'cars|truck'::text THEN 'Vehicles'::text
            WHEN (r.k).key ~ 'peds'::text THEN 'Pedestrians'::text
            WHEN (r.k).key ~ 'bike'::text THEN 'Cyclists'::text
            ELSE NULL::text
        END AS class_type,
    upper("left"((r.k).key, 1)) AS leg,
        CASE
            WHEN "right"((r.k).key, 1) = 'r'::text THEN 'Right'::text
            WHEN "right"((r.k).key, 1) = 't'::text THEN 'Through'::text
            WHEN "right"((r.k).key, 1) = 'l'::text THEN 'Left'::text
            ELSE NULL::text
        END AS movement,
    sum((r.k).value::integer) AS volume_15min
   FROM ( SELECT q.j ->> 'id'::text AS id,
            json_each_text(q.j) AS k
           FROM ( SELECT row_to_json(det.*) AS j
                   FROM traffic.det) q) r
  WHERE ((r.k).key <> ALL (ARRAY['id'::text, 'count_info_id'::text, 'count_time'::text])) AND (r.k).key ~ 'cars|truck|peds|bike'::text
  GROUP BY r.id, (
        CASE
            WHEN (r.k).key ~ 'cars|truck'::text THEN 'Vehicles'::text
            WHEN (r.k).key ~ 'peds'::text THEN 'Pedestrians'::text
            WHEN (r.k).key ~ 'bike'::text THEN 'Cyclists'::text
            ELSE NULL::text
        END), (upper("left"((r.k).key, 1))), (
        CASE
            WHEN "right"((r.k).key, 1) = 'r'::text THEN 'Right'::text
            WHEN "right"((r.k).key, 1) = 't'::text THEN 'Through'::text
            WHEN "right"((r.k).key, 1) = 'l'::text THEN 'Left'::text
            ELSE NULL::text
        END)
WITH DATA;


-- TMC SHORTERM 
DROP MATERIALIZED VIEW IF EXISTS open_data.volumes_tmc_shortterm; 



CREATE MATERIALIZED VIEW open_data.volumes_tmc_shortterm AS 
SELECT node_id AS int_id,
    "substring"("substring"(a.location::text, 'PX \d{1,5}'::text), '\d{1,5}'::text)::INTEGER AS px,
    a.location,
    d.class_type,
    d.leg,
    d.movement,
    b.count_date::date + (c.count_time::time without time zone - '00:15:00'::interval) AS datetime_bin,
    round(avg(d.volume_15min), 0)::integer AS volume_15min
   FROM traffic.arterydata a
     JOIN traffic.countinfomics b USING (arterycode)
     JOIN traffic.det c USING (count_info_id)
     JOIN open_data.flow_tmc_long d USING (id)
     LEFT JOIN gis.traffic_signals g ON g.px = "substring"("substring"(a.location::text, 'PX \d{1,5}'::text), '\d{1,5}'::text)::INTEGER
  WHERE "substring"("substring"(a.location::text, 'PX \d{1,5}'::text), '\d{1,5}'::text) IS NOT NULL
  GROUP BY a.arterycode, a.apprdir, a.location, d.class_type, d.leg, d.movement, b.count_date, c.count_time, g.node_id
 HAVING round(avg(d.volume_15min), 0)::integer > 0
  ORDER BY a.arterycode, b.count_date, c.count_time
WITH DATA;


-- QC

-- check for duplicates
-- reutrns nothing so there are no duplicates 
SELECT int_id, px, datetime_bin, leg, movement, class_type, COUNT(*) 
FROM open_data.volumes_tmc_shortterm
GROUP BY int_id, px, datetime_bin, leg, movement, class_type
HAVING COUNT(*) > 1;


-- check to make sure number of records is identical to source table
SELECT 
(SELECT COUNT(*) FROM open_data.volumes_tmc_shortterm) count_shortterm,
-- query that created vz_challenge.volumes_tmc_shortterm 
(SELECT COUNT(*) FROM (
SELECT "substring"("substring"(a.location::text, 'PX \d{1,5}'::text), '\d{1,5}'::text) AS px,
    'Short Term' AS station_type,
    a.location,
    d.class_type,
    d.leg,
    d.movement,
    b.count_date::date + (c.count_time::time without time zone - '00:15:00'::interval) AS datetime_bin,
    round(avg(d.volume_15min), 0)::integer AS volume_15min
   FROM traffic.arterydata a
     JOIN traffic.countinfomics b USING (arterycode)
     JOIN traffic.det c USING (count_info_id)
     JOIN open_data.flow_tmc_long d USING (id)
  WHERE "substring"("substring"(a.location::text, 'PX \d{1,5}'::text), '\d{1,5}'::text) IS NOT NULL
  GROUP BY a.arterycode, a.apprdir, a.location, d.class_type, d.leg, d.movement, b.count_date, c.count_time
 HAVING round(avg(d.volume_15min), 0)::integer > 0
  ORDER BY a.arterycode, b.count_date, c.count_time  ) x
  ) count_original;


-- check int_id matching is correct 
-- note King / Peter and King / Blue Jays Way are the same intersection (verified by Google Maps)
SELECT DISTINCT node_id int_id, t.px px, t.location, g.main || ' / ' || g.side_1 AS intersection_traffic_signal -- , (SELECT geom from gis.centreline_intersection where int_id = g.node_id) geom
FROM vz_challenge.volumes_tmc_shortterm t LEFT JOIN gis.traffic_signals g ON g.px = t.px::INTEGER; 


