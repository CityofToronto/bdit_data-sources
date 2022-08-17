---------------------------------------------------------------------------------------------------------------------------------------------------
--CRAETE A TABLE FOR ANALYSIS
---------------------------------------------------------------------------------------------------------------------------------------------------
--SELECT *  FROM rbahreh.col_orig_nn_int limit 100;

--DROP TABLE rbahreh.col_analysis;
CREATE TABLE rbahreh.col_analysis 
AS
SELECT inside_city, coord_issue, address_type,
intersec5 as intersection_matched, int_collision_spatil_dist as int_spatial_distance, int_matching_distance as int_text_distance,
collision_add1, collision_add2,collision_add3, add1_add2_trgm_dist as address_text_distance,
distance_knn as nn_spatial_distance, add1_lfname_nn_trgm_dist as nn_text_distance, lf_name as closest_centreline, 
fcode_desc, road_class, loc_type_class,hwy_flag, hwy_add1_flag,
collision_no, KSI, accdate, cl_geom, col_geom, int_geom
FROM rbahreh.col_orig_nn_int;
---------------------------------------------------------------------------------------------------------------------------------------------------
