--INTERSECTION MATCHING

---------------------------------------------------------------------------------------------------------------------------------------------------
--CREATE AN INTERSECTION TABLE
---------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rbahreh.centreline_intersection_streets_oid
AS
SELECT DISTINCT centreline_intersection.objectid,
unnest(string_to_array(centreline_intersection.intersec5::text, '/'::text)) AS street,
centreline_intersection.classifi6,
centreline_intersection.elevatio10
FROM gis.centreline_intersection;

--CREATE INDICES
CREATE INDEX centreline_intersection_oid_btree_idx 
   ON rbahreh.centreline_intersection_streets_oid USING btree (objectid); 
CREATE INDEX centreline_intersection_street_oid_trigram_idx 
   ON rbahreh.centreline_intersection_streets_oid USING GIST (street gist_trgm_ops);    
ANALYSE rbahreh.centreline_intersection_streets_oid;
---------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------
--CREATE INDICES ON COLLISION ADDRESS
---------------------------------------------------------------------------------------------
CREATE INDEX col_orig_nn_collision_add1_trigram_idx  
   ON rbahreh.col_orig_nn USING GIST (collision_add1 gist_trgm_ops);

CREATE INDEX col_orig_nn_collision_add2_trigram_idx 
   ON rbahreh.col_orig_nn USING GIST (collision_add2 gist_trgm_ops);   
ANALYSE rbahreh.col_orig_nn;

---------------------------------------------------------------------------------------------------------------------------------------------------
--INTERSECTION MATCHING
---------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE rbahreh.col_orig_nn_intersections AS
(
	SELECT collision_no, int_id, objectid, distance
	FROM rbahreh.col_orig_nn
	, LATERAL(
		SELECT  intersections.int_id, intersections.objectid , SUM(LEAST(
			intersections.street <-> collision_add1,
			intersections.street <-> collision_add2)) as distance
    	FROM (rbahreh.centreline_intersection_streets_oid LEFT JOIN gis.centreline_intersection USING(objectid)) AS intersections
    	WHERE (collision_add1 <% intersections.street
        	OR collision_add2 <% intersections.street)
    	GROUP BY intersections.int_id, intersections.objectid
    	HAVING COUNT(DISTINCT intersections.street) > 1
    	ORDER BY AVG(
			LEAST(
				intersections.street <-> collision_add1,intersections.street <-> collision_add2)
		)
		LIMIT 1 ) nearest_intersection 
);
---------------------------------------------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------------------------------------
--GET INTERSECTION NAMES
---------------------------------------------------------------------------------------------------------------------------------------------------
--DROP TABLE rbahreh.col_orig_nn_intersection_names;

CREATE TABLE rbahreh.col_orig_nn_intersection_names
AS
SELECT col.*, intersection.intersec5, intersection.geom as int_geom,
ST_Transform(ST_SetSRID(intersection.geom,4326),2952 ) as int_geom_2952
FROM rbahreh.col_orig_nn_intersections col 
LEFT JOIN gis.centreline_intersection intersection
ON col.int_id= intersection.int_id
GROUP BY col.collision_no,col.int_id,col.objectid,col.distance,intersection.intersec5, intersection.geom; --494855
---------------------------------------------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------------------------------------
--JOIN ALL TOGETHER
---------------------------------------------------------------------------------------------------------------------------------------------------
--DROP TABLE rbahreh.col_orig_nn_int;

CREATE TABLE rbahreh.col_orig_nn_int 
AS
SELECT col.collision_no,col.accnb, col.accyear, col.accdate, col.acctime, col.longitude, col.latitude,
 col.geom as col_geom, col.stname1, col.streetype1, col.dir1, col.stname2, col.streetype2, col.dir2, col.stname3, col.streetype3,
 col.dir3, col.road_class, col.location_type, col.location_class, col.collision_type, col.impact_type, col.visibility,
 col.light, col.road_surface_cond, col.px, col.traffic_control, col.traffic_control_cond, col.on_private_property,
 col.description, col.data_source, col.ksi, col.hwy_flag, col.collision_add1, col.collision_add2, col.collision_add3, col.loc_type_class,
 col.inside_city, col.coord_issue, col.hwy_add1_flag, col.address_type, col.add1_add2_trgm_dist, col.col_geom_2952, col.lf_name,
 col.fcode_desc, col.cl_geom, col.distance_knn, col.geo_id, col.cl_geom_2952, col.add1_lfname_nn_trgm_dist,
 int.int_id, int.distance as int_matching_distance, int.intersec5, int.int_geom, int.int_geom_2952
FROM rbahreh.col_orig_nn col
LEFT JOIN rbahreh.col_orig_nn_intersection_names int USING(collision_no);
---------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------
--GEOMETRIC DISTANCE BETWEEN IDENTIFIED INTERSECTION AND COLLISION GEOM
---------------------------------------------------------------------------------------------------------------------------------------------------
ALTER TABLE rbahreh.col_orig_nn_int ADD COLUMN int_collision_spatil_dist numeric;

UPDATE rbahreh.col_orig_nn_int
SET int_collision_spatil_dist = col_geom_2952 <-> int_geom_2952;
---------------------------------------------------------------------------------------------------------------------------------------------------