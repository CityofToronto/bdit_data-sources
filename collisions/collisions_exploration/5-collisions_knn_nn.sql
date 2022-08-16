--GET THE NEAREST NEIGHBOUR

---------------------------------------------------------------------------------------------------------------------------------------------------
--K NEAREST NEIGHBOUR (K=4)
---------------------------------------------------------------------------------------------------------------------------------------------------
SELECT DISTINCT fcode_desc FROM gis.centreline ORDER BY fcode_desc;-- To choose all transportation related centrelines

--DROP TABLE rbahreh.col_orig_abbr_Prj;
CREATE TABLE rbahreh.col_orig_abbr_Prj AS (
	SELECT * , ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude),4326), 2952) AS col_geom_2952
	FROM rbahreh.col_orig_abbr);
    
CREATE INDEX col_orig_abbr_Prj_spatial_idx ON rbahreh.col_orig_abbr_Prj USING GIST(col_geom_2952);
ANALYSE rbahreh.col_orig_abbr_Prj;

--SELECT * FROM rbahreh.col_orig_abbr_Prj LIMIT 1;

--DROP TABLE rbahreh.col_orig_knn;
CREATE TABLE rbahreh.col_orig_knn AS
(
	SELECT col.*, cl.lf_name, cl.fcode_desc, cl.geom as cl_geom, cl.distance_knn, cl.geo_id, cl.cl_geom_2952
	FROM rbahreh.col_orig_abbr_Prj col
	CROSS JOIN LATERAL
	(
		SELECT cl.lf_name, cl.fcode_desc, cl.geom , cl.geo_id, cl.cl_geom_2952, cl.cl_geom_2952 <-> col.col_geom_2952 AS distance_knn
  		FROM (
			SELECT * , ST_Transform(geom, 2952) as cl_geom_2952
			FROM gis.centreline
			WHERE fcode_desc IN ('Expressway', 'Expressway Ramp', 'Major Arterial',
								 'Major Arterial Ramp', 'Minor Arterial', 'Minor Arterial Ramp',
								 'Collector', 'Collector Ramp',  'Local', 'Laneway', 'Pending',
								 'Walkway', 'Trail', 'Other Ramp')
		) AS cl
		ORDER BY distance_knn ASC
  		LIMIT 4
	) cl
); -- 1 min 45s

SELECT COUNT (*) FROM rbahreh.col_orig_knn; --2476648
SELECT COUNT(*) FROM rbahreh.col_orig_knn WHERE distance_knn IS NULL;--60 i.e. 15 collisions are null
---------------------------------------------------------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------------------------------------------------------
--CLOSEST (1st nn)
---------------------------------------------------------------------------------------------------------------------------------------------------
--DROP TABLE rbahreh.col_orig_nn;
CREATE TABLE rbahreh.col_orig_nn AS 
(
	SELECT DISTINCT ON (collision_no) *
	FROM rbahreh.col_orig_knn
	ORDER BY collision_no, distance_knn ASC NULLS LAST
);

--SELECT COUNT(*) FROM rbahreh.col_orig_nn WHERE distance_knn IS NULL;--15
---------------------------------------------------------------------------------------------------------------------------------------------------