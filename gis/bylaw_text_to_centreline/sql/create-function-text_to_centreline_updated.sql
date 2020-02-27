DROP FUNCTION jchew.text_to_centreline_updated(TEXT, TEXT, TEXT);
CREATE OR REPLACE FUNCTION jchew.text_to_centreline_updated(highway TEXT, frm TEXT, t TEXT)
RETURNS TABLE(geo_id NUMERIC, centreline_name VARCHAR, con TEXT, notice TEXT, line_geom GEOMETRY, oid1_geom GEOMETRY, oid1_geom_translated GEOMETRY, oid2_geom geometry, oid2_geom_translated GEOMETRY) AS $$
DECLARE


--STEP 1
	-- clean data


	-- when the input was btwn instead of from and to

	btwn1_v1 TEXT := CASE WHEN t IS NULL THEN
	gis.abbr_street2(regexp_REPLACE
	(regexp_REPLACE
	(regexp_REPLACE
	(split_part
	(split_part
	(regexp_REPLACE
	(regexp_REPLACE(frm, '[0123456789.,]* metres (north|south|east|west|East|northeast|northwest|southwest|southeast) of ', '', 'g')
	, '\(.*?\)', '', 'g')
	, ' to ', 1)
	, ' and ', 1)
	, '\(.*\)', '', 'g')
	, '[Bb]etween ', '', 'g')
	, 'A point', '', 'g'))
	
	ELSE gis.abbr_street2(regexp_REPLACE
	(regexp_REPLACE
	(regexp_REPLACE(frm, '\(.*?\)', '', 'g')
	, '[0123456789.,]* metres (north|south|east|west|East|northeast|northwest|southwest|southeast) of ', '', 'g')
	, 'A point', '', 'g'))
	END;

	-- cases when three roads intersect at once (i.e. btwn1_v1 = Terry Dr/Woolner Ave) ... just take first street
	btwn1 TEXT := (CASE WHEN btwn1_v1 LIKE '%/%'
			THEN split_part(btwn1_v1, '/', 1)
			WHEN btwn1_v1 LIKE '% of %' THEN split_part(btwn1_v1, ' of ', 2)
			ELSE btwn1_v1
			END
	);

	btwn2_orig_v1 TEXT := CASE WHEN t IS NULL THEN
			(CASE WHEN split_part(regexp_REPLACE(frm,  '\(.*?\)', '', 'g'), ' and ', 2) <> ''
			THEN gis.abbr_street2(regexp_REPLACE
			(regexp_REPLACE
			(regexp_REPLACE
			(split_part
			(regexp_REPLACE
			(regexp_REPLACE
			(regexp_REPLACE(frm, '\(.*?\)', '', 'g')
			, '[0123456789.,]* metres (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) of ', '', 'g')
			, 'the (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) end of', '', 'g')
			, ' and ', 2)
			, '[Bb]etween ', '', 'g')
			, 'A point', '', 'g')
			, 'the northeast of', '', 'g'))

			WHEN split_part(frm, ' to ', 2) <> ''
			THEN gis.abbr_street2(regexp_REPLACE
			(regexp_REPLACE
			(split_part
			(regexp_REPLACE
			(regexp_REPLACE
			(regexp_REPLACE(frm, '\(.*?\)', '', 'g')
			, '[0123456789.,]* metres (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) of ', '', 'g')
			, 'the (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) end of', '', 'g')
			, ' to ', 2)
			, '[Bb]etween ', '', 'g')
			, 'A point', '', 'g'))
			END)

			ELSE
			gis.abbr_street2(regexp_REPLACE
			(regexp_REPLACE
			(regexp_REPLACE
			(regexp_REPLACE(t, '\(.*?\)', '', 'g')
			, '[0123456789.]* metres (north|south|east|west|East|northeast|northwest|southwest|southeast|south west) of ', '', 'g')
			, 'the (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) end of', '', 'g')
			, 'the northeast of', '', 'g'))
			END ;

	-- cases when three roads intersect at once (i.e. btwn2_orig_v1 = Terry Dr/Woolner Ave)
	-- if there is still a '/' after cresting btwn2_orig_v1 then we know there are 3 intersections in one
	btwn2_orig TEXT := (CASE WHEN btwn2_orig_v1 LIKE '%/%'
			THEN split_part(btwn2_orig_v1, '/', 1)
			ELSE btwn2_orig_v1
			END
	);

	highway2 TEXT :=  gis.abbr_street2(highway);

	direction_btwn1 TEXT := CASE WHEN t IS NULL THEN
				(
				CASE WHEN btwn1 LIKE '% m %'
				OR gis.abbr_street2( regexp_REPLACE
				(split_part
				(split_part
				(frm, ' to ', 1)
				, ' and ', 1)
				, '[Bb]etween ', '', 'g')) LIKE '% m %'
				THEN split_part
				(split_part
				(gis.abbr_street2
				(regexp_REPLACE
				(regexp_REPLACE
				(regexp_REPLACE
				(split_part
				(split_part
				(frm, ' to ', 1)
				, ' and ', 1)
				, '[Bb]etween ', '', 'g')
				, 'further ', '', 'g')
				, 'east/north', 'northeast', 'g'))
				, ' m ', 2)
				, ' of ', 1)
				ELSE NULL
				END )
				ELSE
				(
				CASE WHEN btwn1 LIKE '% m %'
				OR gis.abbr_street2(frm) LIKE '% m %'
				THEN regexp_replace(regexp_replace
				(split_part
				(split_part
				(gis.abbr_street2(frm), ' m ', 2)
				, ' of ', 1)
				, 'further ', '', 'g')
				, 'east/north', 'northeast', 'g')
				ELSE NULL
				END )
				END;


	direction_btwn2 TEXT := CASE WHEN t IS NULL THEN (
				CASE WHEN btwn2_orig LIKE '% m %'
				OR
				(
					CASE WHEN split_part(frm, ' and ', 2) <> ''
					THEN gis.abbr_street2( regexp_REPLACE(split_part(frm, ' and ', 2), '[Bb]etween ', '', 'g'))
					WHEN split_part(frm, ' to ', 2) <> ''
					THEN gis.abbr_street2( regexp_REPLACE(split_part(frm, ' to ', 2), '[Bb]etween ', '', 'g'))
					END
				) LIKE '% m %'
				THEN
				(
					CASE WHEN split_part(frm, ' and ', 2) <> ''
					THEN regexp_REPLACE(regexp_replace(split_part(split_part( gis.abbr_street2(regexp_REPLACE(split_part(frm, ' and ', 2), '[Bb]etween ', '', 'g')), ' m ', 2), ' of ', 1), 'further ', '', 'g'), 'east/north', 'northeast', 'g')
					WHEN split_part(frm, ' to ', 2) <> ''
					THEN regexp_REPLACE(regexp_replace(split_part(split_part(gis.abbr_street2(regexp_REPLACE(split_part(frm, ' to ', 2), '[Bb]etween ', '', 'g')), ' m ', 2), ' of ', 1), 'further ', '', 'g'), 'east/north', 'northeast', 'g')
					END
				)
				ELSE NULL
				END)
				ELSE
				(
				CASE WHEN btwn2_orig LIKE '% m %'
				OR gis.abbr_street2(t) LIKE '% m %'
				THEN
				regexp_REPLACE(regexp_replace(split_part(split_part(gis.abbr_street2(t), ' m ', 2), ' of ', 1), 'further ', '', 'g'), 'east/north', 'northeast', 'g')
				ELSE NULL
				END
				)
				END;


	metres_btwn1 FLOAT :=	(CASE WHEN t IS NULL THEN
				(
				CASE WHEN btwn1 LIKE '% m %'
				OR gis.abbr_street2(regexp_REPLACE
				(split_part
				(split_part(frm, ' to ', 1)
				, ' and ', 1)
				, 'Between ', '', 'g')) LIKE '% m %'
				THEN regexp_REPLACE
				(regexp_REPLACE
				(regexp_REPLACE
				(split_part
				(gis.abbr_street2
				(regexp_REPLACE
				(split_part
				(split_part(frm, ' to ', 1)
				, ' and ', 1)
				, '[Bb]etween ', '', 'g'))
				, ' m ' ,1)
				, 'a point ', '', 'g')
				, 'A point', '', 'g')
				, ',', '', 'g')::FLOAT
				ELSE NULL
				END
				)
				ELSE
				(
				CASE WHEN btwn1 LIKE '% m %'
				OR gis.abbr_street2(frm) LIKE '% m %'
				THEN regexp_REPLACE
				(regexp_REPLACE
				(regexp_REPLACE
				(split_part
				(gis.abbr_street2(frm), ' m ' ,1)
				, 'a point ', '', 'g')
				, 'A point', '', 'g')
				, ',', 'g')::FLOAT
				ELSE NULL
				END
				)
				END)::FLOAT;


	metres_btwn2 FLOAT :=	(CASE WHEN t IS NULL THEN
				( CASE WHEN btwn2_orig LIKE '% m %' OR
					(
						CASE WHEN split_part(frm, ' and ', 2) <> ''
						THEN gis.abbr_street2( regexp_REPLACE(regexp_REPLACE(split_part(frm, ' and ', 2), '\(.*?\)', '', 'g'), '[Bb]etween ', '', 'g'))
						WHEN split_part(frm, ' to ', 2) <> ''
						THEN gis.abbr_street2( regexp_REPLACE(regexp_REPLACE(split_part(frm, ' to ', 2), '\(.*?\)', '', 'g'), '[Bb]etween ', '', 'g'))
						END
					)
				LIKE '% m %'
				THEN
				(
				CASE WHEN split_part(frm, ' and ', 2) <> ''
				THEN regexp_REPLACE(regexp_REPLACE(regexp_REPLACE(split_part( gis.abbr_street2( regexp_REPLACE(regexp_REPLACE(split_part(frm, ' and ', 2), '\(.*\)', '', 'g'), '[Bb]etween ', '', 'g')), ' m ', 1), 'a point ', '', 'g'), 'A point', '', 'g'), ',', '', 'g')::FLOAT
				WHEN split_part(frm, ' to ', 2) <> ''
				THEN regexp_REPLACE(regexp_REPLACE(regexp_REPLACE(split_part(gis.abbr_street2( regexp_REPLACE(regexp_REPLACE(split_part(frm, ' to ', 2), '\(.*\)', '', 'g'), '[Bb]etween ', '', 'g')), ' m ', 1), 'a point ', '', 'g'), 'A point', '', 'g'), ',', '', 'g')::FLOAT
				END
				)
				ELSE NULL
				END )

				ELSE
				(
				CASE WHEN btwn2_orig LIKE '% m %'
				OR gis.abbr_street2(t) LIKE '% m %'
				THEN
				regexp_REPLACE(regexp_REPLACE(regexp_REPLACE(split_part(gis.abbr_street2(t), ' m ', 1), 'a point ', '', 'g'), 'A point', '', 'g'), ',', '', 'g')::FLOAT
				ELSE NULL
				END
				)
				END)::FLOAT;

	-- to help figure out if the row is case 1
	-- i.e. Watson road from St. Mark's Road to a point 100 metres north
	-- we want the btwn2 to be St. Mark's Road (which is also btwn1)
	-- there are also cases like: street= Arundel Avenue  and  btwn=  Danforth Avenue and a point 44.9 metres north of Fulton Avenue
	-- we need to be able to differentiate the two cases
	-- the difference between the two is that one of the cases has a 'of' to describe the second road that intersects with "street"/"highway2"
	btwn2_check TEXT := CASE WHEN t IS NULL THEN
			(CASE WHEN split_part(frm, ' and ', 2) <> ''
			THEN gis.abbr_street2(regexp_REPLACE(regexp_REPLACE(split_part(frm, ' and ', 2), '[Bb]etween ', '', 'g'), 'A point', '', 'g'))
			WHEN split_part(frm, ' to ', 2) <> ''
			THEN gis.abbr_street2(regexp_REPLACE(regexp_REPLACE(split_part(frm, ' to ', 2), '[Bb]etween ', '', 'g'), 'A point', '', 'g'))
			END)

			ELSE
			gis.abbr_street2(t)
			END ;


	btwn2 TEXT := (
	CASE WHEN btwn2_orig LIKE '%point%' AND (btwn2_check NOT LIKE '% of %' OR btwn2_check LIKE ('% of ' || TRIM(btwn1)))
		-- for case one
		-- i.e. Watson road from St. Mark's Road to a point 100 metres north
		-- we want the btwn2 to be St. Mark's Road (which is also btwn1)
	THEN TRIM(gis.abbr_street2(btwn1))
	ELSE TRIM(gis.abbr_street2(regexp_replace(btwn2_orig , 'a point', '', 'g')))
	END
	);



--STEP 2
	-- get intersection geoms

	text_arr_oid1 TEXT[]:= jchew._get_intersection_geom_updated(highway2, btwn1, direction_btwn1::TEXT, metres_btwn1::FLOAT, 0);

	-- used to keep track of the ID of the first intersection so we don't assign the same intersection twice
	--int_id1 INT := (text_arr_oid1[3])::INT;

	oid1_int INT := (text_arr_oid1[3])::INT;
	oid1_geom GEOMETRY := ST_GeomFromText(text_arr_oid1[1],4326);
	oid1_geom_translated GEOMETRY := ST_GeomFromText(text_arr_oid1[2], 4326);

	text_arr_oid2 TEXT[] := (CASE WHEN btwn2_orig LIKE '%point%' AND (btwn2_check NOT LIKE '% of %' OR btwn2_check LIKE ('% of ' || TRIM(btwn1)))
				THEN jchew._get_intersection_geom_updated(highway2, btwn2, direction_btwn2::TEXT, metres_btwn2::FLOAT, 0)
				ELSE jchew._get_intersection_geom_updated(highway2, btwn2, direction_btwn2::TEXT, metres_btwn2::FLOAT, oid1_int)
				END);

	oid2_int INT := (text_arr_oid2[3])::INT;
	oid2_geom  GEOMETRY := ST_GeomFromText(text_arr_oid2[1], 4326);
	oid2_geom_translated GEOMETRY := ST_GeomFromText(text_arr_oid2[2], 4326);
	

	-- sum of the levenshtein distance of both of the intersections matched
	lev_sum INT := text_arr_oid1[4]::INT + text_arr_oid2[4]::INT;

	-- confidence value
	con TEXT := (
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

-- 	/* To test
-- 	SELECT int_start, int_end, ST_Union(ST_LineMerge(geom)) AS combined_geom
-- 	FROM jchew.get_lines_btwn_interxn(13464376, 13464279)
-- 	GROUP BY int_start, int_end*/

-- /*
-- 	--normal case 
-- 	centreline_segments geometry := (
-- 		CASE 
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

CREATE TEMP TABLE IF NOT EXISTS temp_table AS
        SELECT int_start, int_end, seq, rout.geo_id, lf_name, geom AS line_geom 
		FROM jchew.get_lines_btwn_interxn(oid1_int, oid2_int) rout;


RAISE NOTICE 'btwn1: % btwn2: % btwn2_check: %  highway2: % metres_btwn1: %  metres_btwn2: % direction_btwn1: % direction_btwn2: %', 
btwn1, btwn2, btwn2_check, highway2, metres_btwn1, metres_btwn2, direction_btwn1, direction_btwn2;


RETURN QUERY (SELECT temp_table.geo_id, temp_table.lf_name, con, notice, temp_table.line_geom, oid1_geom, oid1_geom_translated, oid2_geom, oid2_geom_translated FROM temp_table);

END;
$$ LANGUAGE plpgsql;


COMMENT ON FUNCTION jchew.text_to_centreline_updated(highway TEXT, frm TEXT, t TEXT) IS '
The main function for converting text descriptions of locations where bylaws are in effect to centreline segment geometry
Check out README in https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/bylaw_text_to_centreline for more information
';