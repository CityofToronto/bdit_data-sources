DROP FUNCTION jchew.text_to_centreline_updated(INT, TEXT, TEXT, TEXT);
CREATE OR REPLACE FUNCTION jchew.text_to_centreline_updated(_bylaw_id INT, highway TEXT, frm TEXT, t TEXT)
RETURNS TABLE(int1 INTEGER, int2 INTEGER, geo_id NUMERIC, lf_name VARCHAR, con TEXT, note TEXT, 
line_geom GEOMETRY, oid1_geom GEOMETRY, oid1_geom_translated GEOMETRY, oid2_geom geometry, oid2_geom_translated GEOMETRY, 
objectid NUMERIC, fcode INTEGER, fcode_desc VARCHAR) AS $$

DECLARE
	clean_bylaws RECORD;
	an_int_offset RECORD;
	int1_result RECORD;
	int2_result RECORD;
	lev_total INT;
	con TEXT;
	note TEXT;

BEGIN 
--STEP 1 
	-- clean bylaws text
	clean_bylaws := jchew.clean_bylaws_text2(_bylaw_id, highway, frm, t);

--STEP 2
	-- get centrelines geoms
	CREATE TEMP TABLE IF NOT EXISTS _results(
		int_start INT,
		int_end INT,
		seq INT,
		geo_id NUMERIC,
		lf_name VARCHAR,
		line_geom GEOMETRY,
        section NUMRANGE,
		oid1_geom GEOMETRY,
		oid1_geom_translated GEOMETRY,
		oid2_geom GEOMETRY,
		oid2_geom_translated GEOMETRY,
		objectid NUMERIC,
		fcode INT,
		fcode_desc VARCHAR
	);

	IF TRIM(clean_bylaws.btwn1) ILIKE '%entire length%' AND clean_bylaws.btwn2 IS NULL
		THEN
		INSERT INTO _results(geo_id, lf_name, objectid, line_geom, fcode, fcode_desc)
		SELECT *
		FROM jchew._get_entire_length_centreline_segments_updated(clean_bylaws.highway2) ;
		--lev_total := NULL

	--interxn_and_offset
	ELSIF clean_bylaws.btwn1 = clean_bylaws.btwn2
		THEN
		INSERT INTO _results(int_start, geo_id, lf_name, line_geom, section, oid1_geom, oid1_geom_translated, objectid, fcode, fcode_desc)
		SELECT case1.int1, case1.geo_id, case1.lf_name, case1.line_geom, case1.section, 
		case1.oid1_geom, case1.oid1_geom_translated, case1.objectid, case1.fcode, case1.fcode_desc
		FROM jchew._centreline_case1_combined(clean_bylaws.highway2, clean_bylaws.btwn2, clean_bylaws.direction_btwn2, clean_bylaws.metres_btwn2) case1;

		lev_total := (SELECT lev_sum FROM jchew._get_intersection_geom_updated(highway2, btwn2, direction_btwn2, metres_btwn2, 0) );
		--FROM jchew._centreline_case1_combined(clean_bylaws.highway2, clean_bylaws.btwn2, clean_bylaws.direction_btwn2, clean_bylaws.metres_btwn2) ;
  

	--interxns_and_offsets

	ELSE
		int1_result := jchew._get_intersection_geom_updated(clean_bylaws.highway2, clean_bylaws.btwn1, clean_bylaws.direction_btwn1, clean_bylaws.metres_btwn1, 0);

		int2_result := (CASE WHEN clean_bylaws.btwn2_orig LIKE '%point%' AND (clean_bylaws.btwn2_check NOT LIKE '% of %' OR clean_bylaws.btwn2_check LIKE ('% of ' || TRIM(clean_bylaws.btwn1)))
					THEN jchew._get_intersection_geom_updated(clean_bylaws.highway2, clean_bylaws.btwn2, clean_bylaws.direction_btwn2, clean_bylaws.metres_btwn2, 0)
					ELSE jchew._get_intersection_geom_updated(clean_bylaws.highway2, clean_bylaws.btwn2, clean_bylaws.direction_btwn2, clean_bylaws.metres_btwn2, int1_result.int_id_found)
					END);
					
		INSERT INTO _results(int_start, int_end, seq, geo_id, lf_name, line_geom,
        oid1_geom, oid1_geom_translated, oid2_geom, oid2_geom_translated, objectid,	fcode, fcode_desc)
		SELECT int_start, int_end, seq, rout.geo_id, rout.lf_name, geom AS line_geom, 
		int1_result.oid_geom AS oid1_geom, int1_result.oid_geom_translated AS oid1_geom_translated,
		int2_result.oid_geom AS oid2_geom, int2_result.oid_geom_translated AS oid2_geom_translated,
		rout.objectid, rout.fcode, rout.fcode_desc
		FROM jchew.get_lines_btwn_interxn(clean_bylaws.highway2, int1_result.int_id_found, int2_result.int_id_found) rout;

		-- sum of the levenshtein distance of both of the intersections matched
		lev_total := int1_result.lev_sum + int2_result.lev_sum;
	END IF;

	-- confidence value
	con := (
		CASE WHEN TRIM(clean_bylaws.btwn1) ILIKE '%entire length%' AND clean_bylaws.btwn2 IS NULL
		THEN 'Maybe (Entire length of road)'
		WHEN lev_total IS NULL
		THEN 'No Match'
		WHEN lev_total = 0
		THEN 'Very High (100% match)'
		WHEN lev_total = 1
		THEN 'High (1 character difference)'
		WHEN lev_total IN (2,3)
		THEN FORMAT('Medium (%s character difference)', lev_total::TEXT)
		ELSE FORMAT('Low (%s character difference)', lev_total::TEXT)
		END
	);


	note := format('btwn1: %s btwn2: %s highway2: %s metres_btwn1: %s metres_btwn2: %s direction_btwn1: %s direction_btwn2: %s', 
	clean_bylaws.btwn1, clean_bylaws.btwn2, clean_bylaws.highway2, clean_bylaws.metres_btwn1, clean_bylaws.metres_btwn2, 
	clean_bylaws.direction_btwn1, clean_bylaws.direction_btwn2);    

RAISE NOTICE 'btwn1: % btwn2: % btwn2_check: %  highway2: % metres_btwn1: %  metres_btwn2: % direction_btwn1: % direction_btwn2: %', 
clean_bylaws.btwn1, clean_bylaws.btwn2, clean_bylaws.btwn2_check, clean_bylaws.highway2, 
clean_bylaws.metres_btwn1, clean_bylaws.metres_btwn2, clean_bylaws.direction_btwn1, clean_bylaws.direction_btwn2;

RETURN QUERY 
SELECT int_start, int_end, r.geo_id, r.lf_name, con, note, 
r.line_geom, r.oid1_geom, r.oid1_geom_translated, 
r.oid2_geom, r.oid2_geom_translated, 
r.objectid, r.fcode, r.fcode_desc 
FROM _results r;

DROP TABLE _results;

END;
$$ LANGUAGE plpgsql;


COMMENT ON FUNCTION jchew.text_to_centreline_updated(_bylaw_id INT, highway TEXT, frm TEXT, t TEXT) IS '
The main function for converting text descriptions of locations where bylaws are in effect to centreline segment geometry
Check out README in https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/bylaw_text_to_centreline for more information
';
