DROP VIEW IF EXISTS crosic.volumes_tmc_bikes; 
DROP VIEW IF EXISTS open_data.volumes_tmc_bikes; 



CREATE VIEW open_data.volumes_tmc_bikes AS (
SELECT node_id int_id, t.px px, t.location, class_type, leg, movement, datetime_bin, volume_15min
FROM vz_challenge.volumes_tmc_permanent t LEFT JOIN gis.traffic_signals g ON g.px = t.px
WHERE class_type = 'Cyclists' );


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
(SELECT COUNT(*) FROM vz_challenge.volumes_tmc_permanent WHERE class_type = 'Cyclists') count_original;


-- check int_id matching is correct 
-- note King / Peter and King / Blue Jays Way are the same intersection (verified by Google Maps)
SELECT DISTINCT node_id int_id, t.px px, t.location, g.main || ' / ' || g.side_1 AS intersection_traffic_signal -- , (SELECT geom from gis.centreline_intersection where int_id = g.node_id) geom
FROM vz_challenge.volumes_tmc_permanent t LEFT JOIN gis.traffic_signals g ON g.px = t.px
WHERE class_type = 'Cyclists'; 




-- TMC PERMANENT
DROP VIEW IF EXISTS open_data.volumes_tmc_permanent; 



CREATE VIEW open_data.volumes_tmc_permanent AS (
SELECT node_id int_id, t.px px, t.location, class_type, leg, movement, datetime_bin, volume_15min
FROM vz_challenge.volumes_tmc_permanent t LEFT JOIN gis.traffic_signals g ON g.px = t.px
 );


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
(SELECT COUNT(*) FROM vz_challenge.volumes_tmc_permanent) count_original;


-- check int_id matching is correct 
-- note King / Peter and King / Blue Jays Way are the same intersection (verified by Google Maps)
SELECT DISTINCT node_id int_id, t.px px, t.location, g.main || ' / ' || g.side_1 AS intersection_traffic_signal -- , (SELECT geom from gis.centreline_intersection where int_id = g.node_id) geom
FROM vz_challenge.volumes_tmc_permanent t LEFT JOIN gis.traffic_signals g ON g.px = t.px; 




-- TMC SHORTERM 
DROP VIEW IF EXISTS open_data.volumes_tmc_shortterm; 



CREATE VIEW open_data.volumes_tmc_shortterm AS (
SELECT node_id int_id, t.px::INTEGER px, t.location, class_type, leg, movement, datetime_bin, volume_15min
FROM vz_challenge.volumes_tmc_shortterm t LEFT JOIN gis.traffic_signals g ON g.px = t.px::INTEGER
 );


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
(SELECT COUNT(*) FROM vz_challenge.volumes_tmc_shortterm) count_original;


-- check int_id matching is correct 
-- note King / Peter and King / Blue Jays Way are the same intersection (verified by Google Maps)
SELECT DISTINCT node_id int_id, t.px px, t.location, g.main || ' / ' || g.side_1 AS intersection_traffic_signal -- , (SELECT geom from gis.centreline_intersection where int_id = g.node_id) geom
FROM vz_challenge.volumes_tmc_shortterm t LEFT JOIN gis.traffic_signals g ON g.px = t.px::INTEGER; 








