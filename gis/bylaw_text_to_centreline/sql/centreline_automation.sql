CREATE OR REPLACE FUNCTION gis.text_to_centreline(highway TEXT, frm TEXT, t TEXT)
RETURNS TABLE(centreline_segments geometry, con TEXT, street_name_arr TEXT[], ratio NUMERIC, notice TEXT, line_geom geometry, oid1_geom geometry, oid2_geom geometry) AS $$
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

	text_arr_oid1 TEXT[]:= gis._get_intersection_geom(highway2, btwn1, direction_btwn1::TEXT, metres_btwn1::FLOAT, 0);

	-- used to keep track of the ID of the first intersection so we don't assign the same intersection twice
	int_id1 INT := (text_arr_oid1[2])::INT;

	oid1_geom GEOMETRY := ST_GeomFromText(text_arr_oid1[1], 2952);

	text_arr_oid2 TEXT[] := (CASE WHEN btwn2_orig LIKE '%point%' AND (btwn2_check NOT LIKE '% of %' OR btwn2_check LIKE ('% of ' || TRIM(btwn1)))
				THEN gis._get_intersection_geom(highway2, btwn2, direction_btwn2::TEXT, metres_btwn2::FLOAT, 0)
				ELSE gis._get_intersection_geom(highway2, btwn2, direction_btwn2::TEXT, metres_btwn2::FLOAT, int_id1)
				END);

	oid2_geom  GEOMETRY := ST_GeomFromText(text_arr_oid2[1], 2952);



**************
	-- create a line between the two intersection geoms
	line_geom geometry := (CASE WHEN oid1_geom IS NOT NULL AND oid2_geom IS NOT NULL
						  THEN gis._get_line_geom(oid1_geom, oid2_geom)
						   ELSE NULL END);

	match_line_to_centreline_geom geometry := (SELECT geom FROM gis._match_line_to_centreline(line_geom, highway2, metres_btwn1, metres_btwn2));

	match_line_to_centreline_ratio NUMERIC;

	match_line_to_centreline_street_name_arr TEXT[];

	-- match the lines to centreline segments
	centreline_segments geometry := (
				CASE WHEN (TRIM(btwn1) IN ('Entire length', 'Entire Length', 'entire length' , 'The entire length')) AND btwn2 IS NULL
				THEN (SELECT * FROM gis._get_entire_length_centreline_segments(highway2) LIMIT 1)

				WHEN line_geom IS NULL THEN NULL

				WHEN COALESCE(metres_btwn1, metres_btwn2) IS NULL
				THEN
				(
					SELECT *
					FROM ST_LineMerge(match_line_to_centreline_geom)
				)
				-- special case 1
				WHEN btwn1 = btwn2
				THEN
				(
					gis._centreline_case1(direction_btwn2, metres_btwn2, ST_MakeLine(ST_LineMerge(match_line_to_centreline_geom)), line_geom,
					ST_GeomFromText((gis._get_intersection_geom(highway2, btwn1, NULL::TEXT, NULL::FLOAT, 0))[1], 2952) )
				)

				-- special case 2
				ELSE
				(
					gis._centreline_case2(direction_btwn1, direction_btwn2, metres_btwn1, metres_btwn2, match_line_to_centreline_geom, line_geom,
					-- get the original intersection geoms (not the translated ones)
					ST_GeomFromText((gis._get_intersection_geom(highway2, btwn1, NULL::TEXT, NULL::FLOAT, 0))[1], 2952),(CASE WHEN btwn2_orig LIKE '%point%' AND (btwn2_check NOT LIKE '% of %' OR btwn2_check LIKE ('% of ' || TRIM(btwn1)))
					THEN ST_GeomFromText((gis._get_intersection_geom(highway2, btwn2, NULL::TEXT, NULL::FLOAT, 0))[1], 2952)
					ELSE ST_GeomFromText((gis._get_intersection_geom(highway2, btwn2, NULL::TEXT, NULL::FLOAT, int_id1))[1], 2952)
					END))
				)
				END
				);


	-- sum of the levenshtein distance of both of the intersections matched
	lev_sum INT := text_arr_oid1[3]::INT + text_arr_oid2[3]::INT;

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


	notice TEXT := format('btwn1: %s btwn2: %s highway2: %s metres_btwn1: %s metres_btwn2: %s direction_btwn1: %s direction_btwn2: %s oid1: %s oid2: %s', btwn1, btwn2, highway2,
	metres_btwn1, metres_btwn2, direction_btwn1, direction_btwn2, int_id1, (text_arr_oid2[2])::INT);

BEGIN


SELECT tbl.ratio, tbl.street_name_arr
INTO match_line_to_centreline_ratio, match_line_to_centreline_street_name_arr
FROM gis._match_line_to_centreline(line_geom, highway2, metres_btwn1, metres_btwn2) AS tbl;

raise notice 'btwn1: % btwn2: % btwn2_check: %  highway2: % metres_btwn1: %  metres_btwn2: % direction_btwn1: % direction_btwn2: % cntreline_segments: %', btwn1, btwn2, btwn2_check, highway2, metres_btwn1, metres_btwn2, direction_btwn1, direction_btwn2, ST_ASText(ST_Transform(centreline_segments, 4326));


RETURN QUERY (SELECT centreline_segments, con, match_line_to_centreline_street_name_arr AS street_name_arr, match_line_to_centreline_ratio AS ratio, notice, line_geom, oid1_geom, oid2_geom);


END;
$$ LANGUAGE plpgsql;


COMMENT ON FUNCTION gis.text_to_centreline(highway TEXT, frm TEXT, t TEXT) IS '
The main function for converting text descriptions of locations where bylaws are in effect to centreline segment geomtry

Check out README in https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/bylaw_text_to_centreline for more information
';


-- tables/functions that I used to test the function

/*


-- assumes highway_arr and btwn_arr are the same size
CREATE OR REPLACE FUNCTION crosic.make_geom_table(highway_arr TEXT[], frm_arr TEXT[], to_arr TEXT[])
RETURNS TABLE (
highway TEXT,
frm TEXT,
t TEXT,
confidence TEXT,
geom TEXT
)
AS $$
BEGIN

DROP TABLE IF EXISTS inputs;
CREATE TEMP TABLE inputs AS (
SELECT UNNEST(highway_arr) AS highway, UNNEST(frm_arr) AS frm, UNNEST(to_arr) AS t
);


RETURN QUERY
SELECT i.highway, i.frm AS from, i.t AS to, (SELECT con FROM gis.text_to_centreline(i.highway, i.frm, i.t)) AS confidence,
(SELECT centreline_segments FROM gis.text_to_centreline(i.highway, i.frm, i.t)) AS geom

FROM inputs i;


END;
$$ LANGUAGE plpgsql;



-- old version with btwn instead of from and to
DROP TABLE IF EXISTS crosic.centreline_geoms_test;
SELECT *
INTO crosic.centreline_geoms_test
FROM crosic.make_geom_table(ARRAY['Watson Avenue', 'Watson Avenue', 'Watson Avenue', 'North Queen Street'],
ARRAY['Between St Marks Road and St Johns Road', 'Between St Marks Road and a point 100 metres north', 'Between St Marks Road and 100 metres north of St Johns Road', 'Between Shorncliffe Road and Kipling Ave'],
ARRAY[NULL, NULL, NULL, NULL]);


/*
DROP TABLE IF EXISTS crosic.centreline_geoms_test;
SELECT *
INTO crosic.centreline_geoms_test
FROM crosic.make_geom_table(ARRAY['Watson Avenue', 'Watson Avenue', 'Watson Avenue', 'North Queen Street'],
ARRAY['St Marks Road', 'St Marks Road', 'St Marks Road', 'Shorncliffe Road'],
ARRAY['St Johns Road', 'a point 100 metres north', '100 metres north of St Johns Road', 'Kipling Ave'] );
*/


*/
