CREATE OR REPLACE FUNCTION gis._get_intersection_id(highway2 TEXT, btwn TEXT, not_int_id INT)
RETURNS INT[] AS $$
DECLARE
oid INT;
lev_sum INT;
int_id_found INT;

BEGIN
SELECT intersections.objectid, SUM
(LEAST
(levenshtein
(TRIM(intersections.street), TRIM(highway2), 1, 1, 1)
, levenshtein(TRIM(intersections.street)
, TRIM(btwn), 1, 1, 1)))
, intersections.int_id
INTO oid, lev_sum, int_id_found
FROM
(gis.centreline_intersection_streets LEFT JOIN gis.centreline_intersection USING(objectid)) AS intersections


WHERE (levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1) < 4 
OR levenshtein(TRIM(intersections.street), TRIM(btwn), 1, 1, 1) < 4) 
AND intersections.int_id  <> not_int_id


GROUP BY intersections.objectid, intersections.int_id
HAVING COUNT(DISTINCT TRIM(intersections.street)) > 1
ORDER BY AVG(LEAST(levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1), levenshtein(TRIM(intersections.street),  TRIM(btwn), 1, 1, 1)))

LIMIT 1;

raise notice 'highway2 being matched: % btwn being matched: % not_int_id: % intersection arr: %', highway2, btwn, not_int_id, ARRAY[oid, lev_sum];

RETURN ARRAY[oid, lev_sum, int_id_found];

END;
$$ LANGUAGE plpgsql;


COMMENT ON FUNCTION gis._get_intersection_id(TEXT, TEXT, INT) IS '
Input two street names of streets that intersect each other, and 0 or an intersection id that you do not want the function to return
(i.e. sometimes two streets intersect each other twice so if you want to get both intersections by calling this function you would input the first returned intersection id
into the function on the second time the function is called).
This function returns the objectid and intersection id of the intersection, as well as how close the match was. Closeness is measued by levenshtein distance.' ;



-- get intersection id when intersection is a cul de sac or a dead end or a pseudo intersection
-- in these cases the intersection would just be the name of the street
-- some cases have a road that starts and ends in a cul de sac, so not_int_id is the intersection_id of the
-- first intersection in the bylaw text (or 0 if we are trying to find the first intersection).
-- not_int_id is there so we dont ever get the same intersections matched twice
CREATE OR REPLACE FUNCTION gis._get_intersection_id_highway_equals_btwn(highway2 TEXT, btwn TEXT, not_int_id INT)
RETURNS INT[] AS $$
DECLARE
oid INT;
lev_sum INT;
int_id_found INT;

BEGIN
SELECT intersections.objectid, SUM(LEAST(levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1))), intersections.int_id
INTO oid, lev_sum, int_id_found
FROM
(gis.centreline_intersection_streets LEFT JOIN gis.centreline_intersection USING(objectid, classifi6, elevatio10)) AS intersections


WHERE levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1) < 4 
AND intersections.int_id  <> not_int_id 
AND intersections.classifi6 IN ('SEUML','SEUSL', 'CDSSL', 'LSRSL', 'MNRSL')


GROUP BY intersections.objectid, intersections.int_id, elevatio10
ORDER BY AVG(LEAST(levenshtein
(TRIM(intersections.street), TRIM(highway2), 1, 1, 1)
, levenshtein(TRIM(intersections.street)
,  TRIM(btwn), 1, 1, 1))),
(CASE WHEN elevatio10='Cul de sac' THEN 1 
WHEN elevatio10='Pseudo' THEN 2 
WHEN elevatio10='Laneway' THEN 3 ELSE 4 END),
(SELECT COUNT(*) FROM gis.centreline_intersection_streets WHERE objectid = intersections.objectid)

LIMIT 1;

raise notice '(highway=btwn) highway2 being matched: % btwn being matched: % not_int_id: % intersection arr (oid lev sum): %', highway2, btwn, not_int_id, ARRAY[oid, lev_sum];

RETURN ARRAY[oid, lev_sum, int_id_found];

END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gis._get_intersection_id_highway_equals_btwn(TEXT, TEXT, INT) IS '
Get intersection id from a text street name when intersection is a cul de sac or a dead end or a pseudo intersection.
In these cases the intersection name (intersec5 of gis.centreline_intersection) would just be the name of the street.

Input two street names of streets that intersect each other, and 0 or an intersection id that you do not want the function to return
(i.e. sometimes two streets intersect each other twice so if you want to get both intersections by calling this function you would input the
first returned intersection id into the function on the second time the function is called).
This function returns the objectid and intersection id of the intersection, as well as how close the match was. Closeness is measued by levenshtein distance.' ;




CREATE OR REPLACE FUNCTION gis._translate_intersection_point(oid_geom GEOMETRY, metres FLOAT, direction TEXT)
RETURNS GEOMETRY AS $translated_geom$
DECLARE
translated_geom TEXT := (
	(CASE WHEN TRIM(direction) = 'west' THEN ST_AsText(ST_Translate(oid_geom, -metres, 0))
	WHEN TRIM(direction) = 'east' THEN ST_AsText(ST_Translate(oid_geom, metres, 0))
	WHEN TRIM(direction) = 'north' THEN ST_AsText(ST_Translate(oid_geom, 0, metres))
	WHEN TRIM(direction) = 'south' THEN ST_AsText(ST_Translate(oid_geom, 0, -metres))
	WHEN TRIM(direction) IN ('southwest', 'south west') THEN ST_AsText(ST_Translate(oid_geom, -cos(45)*metres, -sin(45)*metres))
	WHEN TRIM(direction) IN ('southeast', 'south east') THEN ST_AsText(ST_Translate(oid_geom, cos(45)*metres, -sin(45)*metres))
	WHEN TRIM(direction) IN ('northwest', 'north west') THEN ST_AsText(ST_Translate(oid_geom, -cos(45)*metres, sin(45)*metres))
	WHEN TRIM(direction) IN ('northeast', 'north east') THEN ST_AsText(ST_Translate(oid_geom, cos(45)*metres, sin(45)*metres))
	END)
	);

BEGIN
RETURN translated_geom;
END;
$translated_geom$ LANGUAGE plpgsql;


COMMENT ON FUNCTION gis._translate_intersection_point(oid_geom GEOMETRY, metres FLOAT, direction TEXT) IS '
Inputs are the geometry of a point, the number of units that you would like the point to be translated, and the direction that you would like the point to be translated.
The function returns the input point geometry translated in the direction inputted, by the number of metres inputted.
Note: if you would like to move your geometry by a certian number of metres, make sure the point geometry has a projection that has metres as the unit.
If the input geometry is unprojected (i.e. SRID = 4326) then the function returns a geometry that has been translated by the number of degrees inputted.
';


--CHANGED
DROP FUNCTION jchew._get_intersection_geom_updated(text, text, text, double precision, integer);
CREATE OR REPLACE FUNCTION jchew._get_intersection_geom_updated(highway2 TEXT, btwn TEXT, direction TEXT, metres FLOAT, not_int_id INT)
RETURNS TEXT[] 
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
geom TEXT;
int_arr INT[] := (CASE WHEN TRIM(highway2) = TRIM(btwn) 
	THEN (gis._get_intersection_id_highway_equals_btwn(highway2, btwn, not_int_id))
	ELSE (gis._get_intersection_id(highway2, btwn, not_int_id))
	END);

oid INT := int_arr[1];
int_id_found INT := int_arr[3];
lev_sum INT := int_arr[2];
oid_geom geometry := (
		SELECT ST_Transform(ST_SetSRID(gis.geom, 4326), 2952)
		FROM gis.centreline_intersection gis
		WHERE objectid = oid
		);
oid_geom_translated geometry := (
		CASE WHEN direction IS NOT NULL OR metres IS NOT NULL
	   	THEN gis._translate_intersection_point(oid_geom, metres, direction) 
		ELSE NULL
		END
		);
-- normal case
arr1 TEXT[] :=  ARRAY(SELECT (
	ST_AsText(ST_Transform(oid_geom,4326))
));
-- special case (create a new column for that)
arr2 TEXT[] :=  ARRAY(SELECT (
	ST_AsText(ST_Transform(ST_SetSRID(oid_geom_translated, 2952),4326))
));
arr3 TEXT[] := arr1 || arr2;
arr4 TEXT[] := ARRAY_APPEND(arr3, int_id_found::TEXT);
arr TEXT[] := ARRAY_APPEND(arr4, lev_sum::TEXT);
BEGIN

raise notice 'get_intersection_geom stuff: oid: % geom: % geom_translated: % direction % metres % not_int_id: %', 
oid, arr1, arr2, direction, metres::TEXT, not_int_id;

RETURN arr;

END;
$BODY$;

COMMENT ON FUNCTION jchew._get_intersection_geom_updated(TEXT, TEXT, TEXT, FLOAT, INT) IS '
Input values of the names of two intersections, direction (may be NULL), number of units the intersection should be translated,
and the intersection id of an intersection that you do not want the function to return (or 0).

Return an array of ST_AsText(oid_geom), ST_AsText(oid_geom_translated) if applicable, int_id_found and lev_sum. 
Outputs an array of the geometry of the intersection described. 
If the direction and metres are not null, it will return the point geometry (known as oid_geom_translated),
translated by x metres in the inputted direction. It also returns (in the array) the intersection id and the objectid of the output intersection.
Additionally, the levenshein distance between the text inputs described and the output intersection name is in the output array.
';


******************BELOW ARE REPLACED BY PG_ROUTING
CREATE OR REPLACE FUNCTION gis._get_line_geom(oid1_geom geometry, oid2_geom geometry)
RETURNS GEOMETRY AS $geom$
DECLARE
len INT := ST_LENGTH(ST_MakeLine(oid1_geom, oid2_geom));
geom GEOMETRY :=
	(
	CASE WHEN len > 11
	THEN ST_Transform(ST_MakeLine(oid1_geom, oid2_geom), 2952)
	END
	);
BEGIN

raise notice 'LINE geom: % length of line: %', ST_AsText(ST_Transform(geom, 4326)), len;

RETURN geom;


END;
$geom$ LANGUAGE plpgsql;


COMMENT ON FUNCTION gis._get_line_geom(geometry, geometry) IS '
Return a line between the two input point geometries. If the line has a length of less than 11, then return NULL
';


CREATE OR REPLACE FUNCTION gis._match_line_to_centreline(line_geom geometry, highway2_before_editing text, metres_btwn1 FLOAT, metres_btwn2 FLOAT)
RETURNS TABLE(geom geometry, street_name_arr TEXT[], ratio NUMERIC) AS $$
DECLARE

-- gardner expressway is called different things in the centreline intersection file and the centreline file
-- so edit the name of roads that
highway2 TEXT := CASE WHEN TRIM(highway2_before_editing) LIKE 'GARDINER EXPRESS%'
	THEN 'F G Gardiner Xy W'
	WHEN TRIM(highway2_before_editing) LIKE '%ALLEN RD%'
	THEN 'William R Allen Rd S'
	ELSE highway2_before_editing
	END;

street_name_arr TEXT[] := (SELECT ARRAY_AGG(lf_name)
					 FROM
					 (
						SELECT DISTINCT lf_name, levenshtein(LOWER(highway2), LOWER(s.lf_name), 1, 1, 2) as lev_dist
						FROM gis.centreline s
						WHERE levenshtein(LOWER(highway2), LOWER(s.lf_name), 1, 1, 2) < 3
						AND
							(
							(
								-- "normal" case ... i.e. one intersection to another
								-- metres_btwn and metres_btwn2 are null
								COALESCE(metres_btwn1, metres_btwn2) IS NULL AND
								-- 10* used to be 3
								ST_DWithin( ST_Transform(s.geom, 2952), ST_BUFFER(line_geom, 3*ST_LENGTH(line_geom), 'endcap=flat join=round') , 10)
								-- over 90% of centreline segment must be in the buffer
								AND ST_Length(st_intersection(ST_BUFFER(line_geom, 3*(ST_LENGTH(line_geom)), 'endcap=flat join=round') , ST_Transform(s.geom, 2952))) /ST_Length(ST_Transform(s.geom, 2952)) > 0.9
							)
							OR
							(
								-- 10 TIMES LENGTH WORKS .... LOOK INTO POTENTIALLY CHANGING LATER
								COALESCE(metres_btwn1, metres_btwn2) IS NOT NULL AND
								ST_DWithin(ST_Transform(s.geom, 2952), ST_BUFFER(line_geom, 10*GREATEST(metres_btwn1, metres_btwn2), 'endcap=flat join=round'), 30)
							)
							)
						ORDER BY levenshtein(LOWER(highway2), LOWER(s.lf_name), 1, 1, 2)
						 ) AS x
				);

street_name TEXT = street_name_arr[1];

geom geometry := (
	SELECT ST_Transform(ST_UNION(s.geom), 2952) -- ST_LineMerge(ST_Transform(ST_UNION(s.geom), 2952))
	FROM gis.centreline s
	WHERE lf_name = street_name
	AND
	(
	(
		-- "normal" case ... i.e. one intersection to another
		-- metres_btwn and metres_btwn2 are null
		COALESCE(metres_btwn1, metres_btwn2) IS NULL AND
		-- 3* could be changed to 10* if you would like
		ST_DWithin( ST_Transform(s.geom, 2952), ST_BUFFER(line_geom, 3*ST_LENGTH(line_geom), 'endcap=flat join=round') , 10)
		AND ST_Length(st_intersection(ST_BUFFER(line_geom, 3*(ST_LENGTH(line_geom)), 'endcap=flat join=round') , ST_Transform(s.geom, 2952))) /ST_Length(ST_Transform(s.geom, 2952)) > 0.9
	)
	OR
	(
		-- 10 TIMES LENGTH WORKS .... LOOK INTO POTENTIALLY CHANGING LATER
		COALESCE(metres_btwn1, metres_btwn2) IS NOT NULL AND
		ST_DWithin(ST_Transform(s.geom, 2952), ST_BUFFER(line_geom, 10*GREATEST(metres_btwn1, metres_btwn2), 'endcap=flat join=round'), 30)
	)
	)
);

-- variable created to help with QC process
-- ratio for len of line_geom to the length of the matched geom
-- should always be less than 1
-- i.e. line_geom is shorter than geom
ratio NUMERIC := ST_Length(line_geom)/ST_Length(ST_LineMerge(geom));

BEGIN

RAISE NOTICE 'centreline chunk of segments: %', ST_AsText(ST_Transform(ST_LineMerge(geom), 4326));

RETURN QUERY (SELECT geom, street_name_arr, ratio);
-- RETURN geom;

END;
$$ LANGUAGE plpgSQL;


COMMENT ON FUNCTION gis._match_line_to_centreline(geometry, text, FLOAT, FLOAT) IS '
Main Steps:
1. Create a large buffer around the line_geom

2. Select centreline segments from gis.centreline that have the right name that are inside of the buffer

Check out README in https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/bylaw_text_to_centreline for more information';


CREATE OR REPLACE FUNCTION gis._centreline_case1(direction_btwn2 text, metres_btwn2 FLOAT, centreline_geom geometry, line_geom geometry, oid1_geom geometry)
RETURNS geometry AS $geom$

-- i.e. St Mark's Ave and a point 100 m north

DECLARE geom geometry := (
-- case where the section of street from the intersection in the specified direction is shorter than x metres
CASE WHEN metres_btwn2 > ST_Length(centreline_geom) AND metres_btwn2 - ST_Length(centreline_geom) < 15
THEN centreline_geom


-- metres_btwn2/ST_Length(d.geom) is the fraction that is supposed to be cut off from the dissolved centreline segment(s)
-- cut off the first fraction of the dissolved line, and the second and check to see which one is closer to the original interseciton

WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)
> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))

THEN ST_LineSubstring(centreline_geom, ST_LineLocatePoint(centreline_geom, oid1_geom) - (metres_btwn2/ST_Length(centreline_geom)),
ST_LineLocatePoint(centreline_geom, oid1_geom))


WHEN ST_LineLocatePoint(centreline_geom, oid1_geom) <
ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
-- take the substring from the intersection to the point x metres ahead of it
THEN ST_LineSubstring(centreline_geom, ST_LineLocatePoint(centreline_geom, oid1_geom),
ST_LineLocatePoint(centreline_geom, oid1_geom) + (metres_btwn2/ST_Length(centreline_geom))  )



END
);

BEGIN

raise notice 'IN CASE 1 FUNCTION !!!!!!!!!!!!!!!!!!!  direction_btwn2: %, metres_btwn2: %  centreline_geom: %  line_geom: %  oid1_geom: % llp1: %  llp2: % len centreline geom: %', direction_btwn2, metres_btwn2, ST_ASText(ST_Transform(centreline_geom, 4326)), ST_AsText(ST_Transform(line_geom, 4326)), ST_AsText(ST_Transform(oid1_geom, 4326)), ST_LineLocatePoint(centreline_geom, oid1_geom),  ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1))), ST_Length(centreline_geom);

RETURN geom;

END;
$geom$ LANGUAGE plpgSQL;


COMMENT ON FUNCTION gis._centreline_case1(text, FLOAT, geometry,geometry, geometry) IS '
Meant to split line geometries of bylaw in effect locations where the bylaw occurs between an intersection and an offset.
Check out README in https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/bylaw_text_to_centreline for more information';



-- get the closest line (when the line is a multi-linestring) to an intersection point
CREATE OR REPLACE FUNCTION gis._get_closest_line(pnt GEOMETRY, multi_line GEOMETRY, line_geom GEOMETRY)
RETURNS TABLE(geom_part GEOMETRY, dist FLOAT, path INT) AS $$


DECLARE geom_part GEOMETRY := (SELECT line_dump.geom AS geom_part
		FROM

		ST_DUMP(multi_line) AS line_dump

		ORDER BY ST_Distance(pnt, line_dump.geom)::FLOAT ASC
		LIMIT 1);

BEGIN

raise notice '(get_closest_line) pnt: %   multiline: %  geom_part to be returned: %', ST_AsText(ST_Transform(pnt, 4326)),
St_AsText(ST_Transform(multi_line, 4326)), St_AsText(ST_Transform(geom_part, 4326));

RETURN QUERY SELECT *
		FROM
			(
			SELECT line_dump.geom AS geom_part, ST_Distance(pnt, line_dump.geom)::FLOAT AS dist,
			line_dump.path[1]::INT AS path
			FROM
			ST_DUMP(multi_line) AS line_dump
			) AS dump
		WHERE ST_Intersects(ST_Buffer(St_LineSubstring(line_geom, 0.01, 0.99), ST_Length(line_geom)*6, 'endcap=flat'), dump.geom_part)
		ORDER BY dist ASC
		LIMIT 1;

END;
$$ LANGUAGE plpgsql;


COMMENT ON FUNCTION gis._get_closest_line(pnt GEOMETRY, multi_line GEOMETRY, line_geom GEOMETRY) IS '
Get the closest line (when the line is a multi-linestring) to an intersection point.

Check out README in https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/bylaw_text_to_centreline for more information
';


-- function that is used to cut line segments for when the input is a multi-linestring
CREATE OR REPLACE FUNCTION gis._cut_closest_line(direction_btwn1 TEXT, direction_btwn2 TEXT, metres_btwn1 FLOAT, metres_btwn2 FLOAT, centreline_geom_not_merged geometry, line_geom geometry, oid1_geom GEOMETRY, oid2_geom GEOMETRY)
RETURNS geometry AS $geom$
DECLARE

centreline_geom geometry := ST_LineMerge(centreline_geom_not_merged);

closest_path_not_merged1 INT := (SELECT path FROM gis._get_closest_line(oid1_geom, centreline_geom_not_merged, line_geom));

closest_line_geom1 GEOMETRY := (SELECT geom_part FROM gis._get_closest_line(oid1_geom, centreline_geom, line_geom) );

closest_line_path1 INT := (SELECT path FROM gis._get_closest_line(oid1_geom, centreline_geom, line_geom));

closest_line_geom2 GEOMETRY := (SELECT geom_part FROM gis._get_closest_line(oid2_geom,centreline_geom, line_geom));

closest_line_path2 INT := (SELECT path FROM gis._get_closest_line(oid2_geom,centreline_geom, line_geom));

closest_path_not_merged2 INT := (SELECT path FROM gis._get_closest_line(oid2_geom,centreline_geom_not_merged, line_geom));

endpoint_line1 GEOMETRY := CASE WHEN closest_line_geom1 IS NULL THEN NULL
			WHEN ST_LineLocatePoint(closest_line_geom1, oid1_geom) < 0.5
			THEN ST_LineInterpolatePoint(closest_line_geom1, 0.99999)
			ELSE ST_LineInterpolatePoint(closest_line_geom1, 0.00001)
			END;

-- Everything from here and down are things that I'm not sure about !
-- especially the creation of new_line1 and new_line2
new_line1 GEOMETRY := (
			CASE WHEN direction_btwn1 IS NOT NULL AND direction_btwn2 IS NULL
			THEN gis._centreline_case2(NULL, direction_btwn1, NULL, metres_btwn1, closest_line_geom1, ST_MakeLine(oid1_geom, ST_SetSRID(gis._translate_intersection_point(oid1_geom, metres_btwn1, direction_btwn1), 2952)), -- ST_MakeLine(oid1_geom, endpoint_line1), -- ST_MakeLine(oid1_geom, oid2_geom),
			oid1_geom, endpoint_line1)
			-- ??????????????? change this part below ???????
			WHEN direction_btwn1 IS NOT NULL AND direction_btwn2 IS NOT NULL
			THEN gis._centreline_case2(NULL, direction_btwn1, NULL, metres_btwn1, closest_line_geom1, ST_MakeLine(oid1_geom, ST_SetSRID(gis._translate_intersection_point(oid1_geom, metres_btwn1, direction_btwn1), 2952)), -- ST_MakeLine(oid1_geom, oid2_geom), --ST_MakeLine(oid1_geom, endpoint_line1),
			oid1_geom, endpoint_line1)
			-- from the point that oid1 intersects the merged centreline segment to the point where the merged centreline segments starts/ends
			ELSE gis._line_substring_lower_value_first(closest_line_geom1, St_LineLocatePoint(closest_line_geom1, oid1_geom), ST_LineLocatePoint(closest_line_geom1, endpoint_line1))
			END);


-- issue here ?? cartwright ave id "681" does not leave the correct part of line
endpoint_line2 GEOMETRY := CASE WHEN closest_line_geom2 IS NULL THEN NULL
			WHEN ST_LineLocatePoint(closest_line_geom2, oid2_geom) < 0.5
			THEN ST_LineInterpolatePoint(closest_line_geom2, 0.99999)
			ELSE ST_LineInterpolatePoint(closest_line_geom2, 0.00001)
			END;


new_line2 GEOMETRY := (
			-- when line 2 is extended or cut short
			CASE WHEN direction_btwn2 IS NOT NULL AND closest_line_path2 <> closest_line_path1
			THEN
			gis._centreline_case2(direction_btwn2, NULL, metres_btwn2, NULL, closest_line_geom2,
									ST_MakeLine(oid2_geom, ST_SetSRID(gis._translate_intersection_point(oid2_geom, metres_btwn2, direction_btwn2), 2952)), -- ST_MakeLine(oid1_geom, endpoint_line1),
									endpoint_line2, oid2_geom)


			WHEN direction_btwn2 IS NOT NULL AND closest_line_path2 = closest_line_path1
			-- if the path is the same that means they are the same line segment
			-- sooo just call case2 like normal on the one line segment
			-- this becomes a normal case 2 example, with closest_line_geom1 or 2 being the line
			THEN gis._centreline_case2(direction_btwn1, direction_btwn2, metres_btwn1, metres_btwn2, closest_line_geom1, line_geom, --ST_MakeLine(oid1_geom, endpoint_line1),
			oid1_geom, oid2_geom)

			-- when direction_btwn2 is null and direction_btwn1 is not null
			-- so newline1 is already a cut version of one of the lines
			-- newline1 and this line will be created from different lines
			-- so the second line will be from the point that oid2 intersects the merged centreline segment to the point where the merged centreline segments starts/ends
			WHEN direction_btwn2 IS NULL AND closest_line_path2 <> closest_line_path1
			THEN gis._line_substring_lower_value_first(closest_line_geom2, St_LineLocatePoint(closest_line_geom2, oid2_geom), ST_LineLocatePoint(closest_line_geom2, endpoint_line2))


			-- when direction_btwn2 is null and direction_btwn1 is not null
			-- newline1 and newline2 will be versions of the same line because
			-- both oid1 and oid2 have the same line that is closest to them
			-- so this line will be from oid2 to the end/start of newline1, S
			WHEN direction_btwn2 IS NULL AND closest_line_path2 = closest_line_path1
			THEN gis._line_substring_lower_value_first(new_line1, St_LineLocatePoint(new_line1, oid2_geom),ST_LineLocatePoint(new_line1, endpoint_line2))

			END );



final_line GEOMETRY :=  (
		SELECT ST_CollectionExtract(ST_Collect(geoms.geom), 2)
		FROM
		(
			SELECT closest_line_path1, new_line1 as geom

		UNION
			SELECT closest_line_path2, new_line2 as geom
		) AS geoms
		)
		;


BEGIN

RAISE NOTICE 'CUT MULTILINE FUNCTION'
'centreline_geom: %  line_geom %'
'closest_line_path1: % closest_line_path2: % closest_line_geom1: % closest_line_geom2: %'
'endpoint_line1 % endpoint_line2 % new_line1 % new_line2 % final_line %',
ST_ASText(ST_Transform(centreline_geom, 4326)), ST_AsText(ST_Transform(line_geom, 4326)), closest_line_path1, closest_line_path2,
ST_ASText(ST_Transform(closest_line_geom1, 4326)), ST_ASText(ST_Transform(closest_line_geom2, 4326)), ST_AsText(ST_Transform(endpoint_line1, 4326)),
ST_AsText(ST_Transform(endpoint_line2, 4326)), ST_AsText(ST_Transform(new_line1, 4326)),
ST_AsText(ST_Transform(new_line2, 4326)),  ST_AsText(ST_Transform(final_line, 4326));

RETURN final_line;



END;
$geom$ LANGUAGE plpgsql;



COMMENT ON FUNCTION gis._cut_closest_line(TEXT, TEXT, FLOAT, FLOAT, geometry, geometry, GEOMETRY, GEOMETRY) IS '
Function that is used when a bylaw is in effect between two intersections and (one or two) offset(s),
and the street is not continuous (i.e .geometry is a multi-linestring). It cuts line segments for when the input is a multi-linestring.

Check out README in https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/bylaw_text_to_centreline for more information
';


-- this function is used to make sure the lowest linelocatepoint fraction values comes first in the function call to st_linesubstring
-- in line substring lower fraction must come before a higher fraction
CREATE OR REPLACE FUNCTION gis._line_substring_lower_value_first(geom geometry, value1 float, value2 float)
RETURNS geometry AS $geom$

DECLARE

geom geometry := (

-- one value may be a value subtracted from another
-- or a value added to another
-- in this case there is a possibility of the fraction being > 1 or < 0
CASE WHEN geom IS NULL THEN NULL
WHEN GREATEST(value1, value2) > 1
THEN ST_LineSubstring(geom, LEAST(value1, value2), 1)

WHEN LEAST(value1, value2) < 0
THEN ST_LineSubstring(geom, 0, GREATEST(value1, value2))

ELSE
ST_LineSubstring(geom, LEAST(value1, value2), GREATEST(value1, value2))

END
);


BEGIN

RETURN geom;

END;
$geom$ LANGUAGE plpgSQL;


COMMENT ON FUNCTION gis._line_substring_lower_value_first(geometry, float, float) IS '
Call ST_LineSubString function, ensuring that the lower float value is inputted into the function first.
';


CREATE OR REPLACE FUNCTION gis._centreline_case2(direction_btwn1 text, direction_btwn2 text, metres_btwn1 FLOAT, metres_btwn2 FLOAT, centreline_geom_not_merged geometry, line_geom geometry, oid1_geom geometry, oid2_geom geometry)
RETURNS geometry AS $geom$
-- i.e. St Marks Ave and 100 metres north of St. John's Ave

DECLARE

centreline_geom geometry := ST_LineMerge(centreline_geom_not_merged);

geom geometry := (
	CASE


	-- check if the geometry is a multi or single line part
	WHEN ST_GeometryType(centreline_geom) = 'ST_MultiLineString'
	THEN gis._cut_closest_line(direction_btwn1, direction_btwn2, metres_btwn1, metres_btwn2, centreline_geom_not_merged, line_geom, oid1_geom, oid2_geom)



	WHEN (metres_btwn2 IS NOT NULL AND metres_btwn2 > ST_Length(centreline_geom) AND metres_btwn2 - ST_Length(centreline_geom) < 15)
	OR (metres_btwn1 IS NOT NULL AND metres_btwn1 > ST_Length(centreline_geom) AND metres_btwn1 - ST_Length(centreline_geom) < 15)
	THEN centreline_geom


	-- Case 1: direction_btwn1 IS NULL
	-- find substring between intersection 1 and the point x metres away from intersection 2
	WHEN direction_btwn1 IS NULL
		THEN (
		-- intersection of oid2 with centreline_geom has a smaller integer value than
		-- intersection with the end of the line_geom (i.e. the translated intersection of oid2) and centreline_geom
		-- since we wanna cut from a point that is very close to the end of line_geom, we need to subtract from oid2's linelocatepoint value
	    -- think of it as we need to oid2 to be a value that is close to the linelocate value of translated oid2
		CASE WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		-- take the line from intersection 1 to x metres before intersection 2
		THEN
		gis._line_substring_lower_value_first(centreline_geom,
		ST_LineLocatePoint(centreline_geom, oid1_geom),
		ST_LineLocatePoint(centreline_geom, oid2_geom) - (metres_btwn2/ST_Length(centreline_geom)))


		WHEN  ST_LineLocatePoint(centreline_geom, oid2_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
			-- intersection of oid2 with centreline_geom has a smaller integer value than
			-- intersection with the end of the line_geom (i.e. the translated intersection of oid2) and centreline_geom
			-- since we wanna cut from a point that is very close to the end of line_geom, we need to add from oid2's linelocatepoint value
			-- think of it as we need to oid2 to be a value that is close to the linelocate value of translated oid2
			THEN
			-- take the line from intersection 1 to x metres after intersection 2
			gis._line_substring_lower_value_first(centreline_geom,
			ST_LineLocatePoint(centreline_geom, oid1_geom),
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom)))


		END
		)


	-- When direction 2 IS NULL
	-- means that the zone is between intersection 2 and x metres away from intersection 1
	WHEN direction_btwn2 IS NULL

		THEN (

		CASE WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0, 0.000001)))
		-- intersection of oid1 with centreline_geom has a greater integer value than
		-- intersection with the beginning of the line_geom (i.e. the translated intersection of oid1) and centreline_geom
		-- since we wanna cut from a point that is very close to the beginning of line_geom, we need to subtract from oid1's linelocatepoint value
	    -- think of it as we need to oid1 to be a value that is close to the linelocate value of translated oid1
			THEN
			-- take the line from intersection 2 to x metres before intersection 1
			gis._line_substring_lower_value_first(centreline_geom,
			ST_LineLocatePoint(centreline_geom, oid1_geom) - (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom))


		WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0, 0.000001)))
			-- intersection of oid1 with centreline_geom has a smaller integer value than
			-- intersection with the beginning of the line_geom (i.e. the translated intersection of oid1) and centreline_geom
			-- since we wanna cut from a point that is very close to the beginning of line_geom, we need to add from oid1's linelocatepoint value
			-- think of it as we need to oid1 to be a value that is close to the linelocate value of translated oid1
			THEN 
			gis._line_substring_lower_value_first(centreline_geom,
			ST_LineLocatePoint(centreline_geom, oid1_geom) + (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom))

		END)


	-- else when both are not null
	-- i.e. the zone is between the point x metres away from int 1 and y metres away from int 2
	ELSE (
		-- both times the intersection occurs after (higher fraction) than the point closest to the end of the rough line
		(CASE WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		AND  ST_LineLocatePoint(centreline_geom, oid1_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, line_geom))
			THEN
			-- so line substring wont give error
			gis._line_substring_lower_value_first(centreline_geom,
			ST_LineLocatePoint(centreline_geom, oid1_geom)- (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom)- (metres_btwn2/ST_Length(centreline_geom)))


		-- both before
		WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		AND  ST_LineLocatePoint(centreline_geom, oid1_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, line_geom))

			THEN
			gis._line_substring_lower_value_first(centreline_geom,
			ST_LineLocatePoint(centreline_geom, oid1_geom)+ (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom)))


		-- int 2 before (+), int 1 after (-)
		WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		AND  ST_LineLocatePoint(centreline_geom, oid1_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, line_geom))
			THEN
			gis._line_substring_lower_value_first(centreline_geom,
			ST_LineLocatePoint(centreline_geom, oid1_geom)- (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom)))


		-- int 2 after(-), int 1 before (+)
		WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		AND  ST_LineLocatePoint(centreline_geom, oid1_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, line_geom))
			THEN
			-- so line substring wont give error
			gis._line_substring_lower_value_first(centreline_geom,
			ST_LineLocatePoint(centreline_geom, oid1_geom) + (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom) - (metres_btwn2/ST_Length(centreline_geom)))


		END
		) )



	END);



BEGIN

raise notice 'IN THE CASE TWO FUNCTION !!!!!';
raise notice 'CASE 2 PARAMETERS  direction_btwn1 %, direction_btwn2 %, metres_btwn1 %, metres_btwn2 %, centreline_geom %, line_geom %, oid1_geom %, oid2_geom % output geom: %',direction_btwn1, direction_btwn2, metres_btwn1, metres_btwn2, ST_AsText(ST_Transform(centreline_geom, 4326)), ST_AsText(ST_Transform(line_geom, 4326)), ST_AsText(ST_Transform(oid1_geom, 4326)), ST_AsText(ST_Transform(oid2_geom, 4326)), ST_AsText(ST_Transform(geom, 4326));

RETURN geom;

END;
$geom$ LANGUAGE plpgSQL;


COMMENT ON FUNCTION gis._centreline_case2(text, text,  FLOAT, FLOAT, geometry, geometry,  geometry, geometry) IS '
Meant to split line geometries of bylaw in effect locations where the bylaw occurs between two intersections and one or two offset(s).
Check out README in https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/bylaw_text_to_centreline for more information';



CREATE OR REPLACE FUNCTION gis._get_entire_length_centreline_segments(highway2_before_editing text)
RETURNS GEOMETRY AS $geom$

-- i.e. "Hemlock Avenue" and "Entire length"

DECLARE


highway2 TEXT :=
	CASE WHEN TRIM(highway2_before_editing) LIKE 'GARDINER EXPRESSWAY%'
	THEN 'F G Gardiner Xy W'
	WHEN highway2_before_editing = 'Don Valley Pky'
	THEN 'Don Valley Parkway'
	ELSE highway2_before_editing
	END;


segments GEOMETRY := (SELECT ST_UNION(geom) FROM gis.centreline WHERE lf_name = highway2);

geom GEOMETRY := CASE WHEN segments IS NOT NULL THEN ST_Transform(ST_LineMerge(segments),2952)
		ELSE NULL END;


BEGIN

RETURN geom;

END;
$geom$ LANGUAGE plpgSQL;


COMMENT ON FUNCTION gis._get_entire_length_centreline_segments(text) IS '
Union and line merge all of the centreline segments in the City with the exact street name of the street name inputted.
Returns geometry projected into the  MTM Zone 10 (SRID = 2952) projection
';
