DROP FUNCTION jchew.text_to_centreline_updated(TEXT, TEXT, TEXT);
CREATE OR REPLACE FUNCTION jchew.text_to_centreline_updated(highway TEXT, frm TEXT, t TEXT)
RETURNS TABLE(int1 INTEGER, int2 INTEGER, geo_id NUMERIC, lf_name VARCHAR, con TEXT, notice TEXT, 
line_geom GEOMETRY, oid1_geom GEOMETRY, oid1_geom_translated GEOMETRY, oid2_geom geometry, oid2_geom_translated GEOMETRY, 
objectid NUMERIC, fcode INTEGER, fcode_desc VARCHAR) AS $$
DECLARE
	clean_bylaws RECORD;
	int1_result RECORD;
	int2_result RECORD;
	lev_sum INT;
	con TEXT;
	_notice TEXT;
--STEP 1 
	-- clean bylaws text
	


--STEP 2
	-- get intersection geoms
	

-- 	/* To test
-- 	SELECT int_start, int_end, ST_Union(ST_LineMerge(geom)) AS combined_geom
-- 	FROM jchew.get_lines_btwn_interxn(13464376, 13464279)
-- 	GROUP BY int_start, int_end*/

-- /*
-- 	--normal case 
-- 	centreline_segments geometry := (
-- 		CASE 
		-- CASE WHEN (TRIM(btwn1) IN ('Entire length', 'Entire Length', 'entire length' , 'The entire length')) AND btwn2 IS NULL
		-- 		THEN (SELECT * FROM gis._get_entire_length_centreline_segments(highway2) LIMIT 1)
-- 		--empty ones
-- 		WHEN line_geom IS NULL THEN NULL
-- 		--normal cases
-- 		WHEN COALESCE(metres_btwn1, metres_btwn2) IS NULL
-- 		THEN line_geom

-- 		-- special case 1
-- 		WHEN btwn1 = btwn2
-- 		THEN
-- 		(
-- 			gis._centreline_case1(direction_btwn2, metres_btwn2, ST_MakeLine(ST_LineMerge(match_line_to_centreline_geom)), line_geom,
-- 			ST_GeomFromText((gis._get_intersection_geom(highway2, btwn1, NULL::TEXT, NULL::FLOAT, 0))[1], 2952) )
-- 		)

-- 		-- special case 2
-- 		ELSE
-- 		(
-- 			gis._centreline_case2(direction_btwn1, direction_btwn2, metres_btwn1, metres_btwn2, match_line_to_centreline_geom, line_geom,
-- 			-- get the original intersection geoms (not the translated ones)
-- 			ST_GeomFromText((gis._get_intersection_geom(highway2, btwn1, NULL::TEXT, NULL::FLOAT, 0))[1], 2952),(CASE WHEN btwn2_orig LIKE '%point%' AND (btwn2_check NOT LIKE '% of %' OR btwn2_check LIKE ('% of ' || TRIM(btwn1)))
-- 			THEN ST_GeomFromText((gis._get_intersection_geom(highway2, btwn2, NULL::TEXT, NULL::FLOAT, 0))[1], 2952)
-- 			ELSE ST_GeomFromText((gis._get_intersection_geom(highway2, btwn2, NULL::TEXT, NULL::FLOAT, int_id1))[1], 2952)
-- 			END))
-- 		)
-- 		END
-- 		);
-- */


BEGIN 

	clean_bylaw := jchew.clean_bylaws_text(highway, frm, t);

	CREATE TEMP TABLE IF NOT EXISTS _results(
		int_start INT,
		int_end INT,
		seq INT,
		geo_id INT,
		lf_name TEXT,
		objectid INT,
		line_geom GEOMETRY,
		fcode INT,
		fcode_desc TEXT

	);

	CASE WHEN (TRIM(btwn1) ILIKE '%entire length%') AND btwn2 IS NULL
		THEN
		INSERT INTO _results
		SELECT NULL AS int_start, NULL AS int_end, NULL AS seq,*
		FROM jchew._get_entire_length_centreline_segments_updated(highway2);
	ELSE
		int1_result := jchew._get_intersection_geom_updated(highway2, btwn1, direction_btwn1::TEXT, metres_btwn1::FLOAT, 0);
	 	
		int2_result := (CASE WHEN btwn2_orig LIKE '%point%' AND (btwn2_check NOT LIKE '% of %' OR btwn2_check LIKE ('% of ' || TRIM(btwn1)))
					THEN jchew._get_intersection_geom_updated(highway2, btwn2, direction_btwn2::TEXT, metres_btwn2::FLOAT, 0)
					ELSE jchew._get_intersection_geom_updated(highway2, btwn2, direction_btwn2::TEXT, metres_btwn2::FLOAT, oid1_int)
					END);
		INSERT INTO _results
		SELECT int_start, int_end, seq, rout.geo_id, rout.lf_name, rout.objectid, geom AS line_geom, rout.fcode, rout.fcode_desc
		FROM jchew.get_lines_btwn_interxn(oid1_int, oid2_int) rout;
	END;

	-- sum of the levenshtein distance of both of the intersections matched
	lev_sum := text_arr_oid1[4]::INT + text_arr_oid2[4]::INT;

	-- confidence value
	con := (
		CASE WHEN (btwn1 = 'Entire length' OR btwn1 ='Entire Length' OR btwn1 = 'entire length') AND btwn2 IS NULL
		THEN 'Low (description was the entire length of road)'
		WHEN lev_sum IS NULL
		THEN 'No Match'
		WHEN lev_sum = 0
		THEN 'Very High (100% match)'
		WHEN lev_sum = 1
		THEN 'High (1 character difference)'
		WHEN lev_sum IN (2,3)
		THEN FORMAT('Medium (%s character difference)', lev_sum::TEXT)
		ELSE FORMAT('Low (%s character difference)', lev_sum::TEXT)
		END
	);


	notice TEXT := format('btwn1: %s btwn2: %s highway2: %s metres_btwn1: %s metres_btwn2: %s direction_btwn1: %s direction_btwn2: %s oid1: %s oid2: %s', 
	btwn1, btwn2, highway2,
	metres_btwn1, metres_btwn2, direction_btwn1, direction_btwn2, oid1_int, oid2_int);    

RAISE NOTICE 'btwn1: % btwn2: % btwn2_check: %  highway2: % metres_btwn1: %  metres_btwn2: % direction_btwn1: % direction_btwn2: %', 
btwn1, btwn2, btwn2_check, highway2, metres_btwn1, metres_btwn2, direction_btwn1, direction_btwn2;


RETURN QUERY (SELECT oid1_int AS int1, oid2_int AS int2, 
temp_table.geo_id, temp_table.lf_name, 
con, notice, temp_table.line_geom, 
oid1_geom, oid1_geom_translated, 
oid2_geom, oid2_geom_translated,
temp_table.objectid, temp_table.fcode, temp_table.fcode_desc
FROM temp_table);

DROP TABLE temp_table;

END;
$$ LANGUAGE plpgsql;


COMMENT ON FUNCTION jchew.text_to_centreline_updated(highway TEXT, frm TEXT, t TEXT) IS '
The main function for converting text descriptions of locations where bylaws are in effect to centreline segment geometry
Check out README in https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/bylaw_text_to_centreline for more information
';