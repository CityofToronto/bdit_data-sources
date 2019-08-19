CREATE OR REPLACE FUNCTION crosic.get_intersection_id(highway2 TEXT, btwn TEXT, not_int_id INT)
RETURNS INT[] AS $$
DECLARE 
oid INT;
lev_sum INT;
int_id_found INT;

BEGIN 
SELECT intersections.objectid, SUM(LEAST(levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1), levenshtein(TRIM(intersections.street), TRIM(btwn), 1, 1, 1))), intersections.int_id
INTO oid, lev_sum, int_id_found
FROM 
(gis.centreline_intersection_streets LEFT JOIN gis.centreline_intersection USING(objectid)) AS intersections 
 

WHERE (levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1) < 4 OR levenshtein(TRIM(intersections.street), TRIM(btwn), 1, 1, 1) < 4) AND intersections.int_id  <> not_int_id


GROUP BY intersections.objectid, intersections.int_id
HAVING COUNT(DISTINCT TRIM(intersections.street)) > 1 
ORDER BY AVG(LEAST(levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1), levenshtein(TRIM(intersections.street),  TRIM(btwn), 1, 1, 1)))

LIMIT 1;

raise notice 'highway2 being matched: % btwn being matched: % not_int_id: % intersection arr: %', highway2, btwn, not_int_id, ARRAY[oid, lev_sum];

RETURN ARRAY[oid, lev_sum, int_id_found]; 

END; 
$$ LANGUAGE plpgsql; 





-- get intersection id when intersection is a cul de sac or a dead end or a pseudo intersection
-- in these cases the intersection would just be the name of the street 
-- some cases have a road that starts and ends in a cul de sac, so not_int_id is the intersection_id of the 
-- first intersection in the bylaw text (or 0 if we are trying to find the first intersection).
-- not_int_id is there so we dont ever get the same intersections matched twice
CREATE OR REPLACE FUNCTION crosic.get_intersection_id_highway_equals_btwn(highway2 TEXT, btwn TEXT, not_int_id INT)
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
 

WHERE levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1) < 4 AND intersections.int_id  <> not_int_id AND intersections.classifi6 IN ('SEUML','SEUSL', 'CDSSL', 'LSRSL', 'MNRSL')


GROUP BY intersections.objectid, intersections.int_id, elevatio10
ORDER BY AVG(LEAST(levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1), levenshtein(TRIM(intersections.street),  TRIM(btwn), 1, 1, 1))), 
(CASE WHEN elevatio10='Cul de sac' THEN 1 WHEN elevatio10='Pseudo' THEN 2 WHEN elevatio10='Laneway' THEN 3 ELSE 4 END),
(SELECT COUNT(*) FROM gis.centreline_intersection_streets WHERE objectid = intersections.objectid) 

LIMIT 1;

raise notice '(highway=btwn) highway2 being matched: % btwn being matched: % not_int_id: % intersection arr (oid lev sum): %', highway2, btwn, not_int_id, ARRAY[oid, lev_sum];

RETURN ARRAY[oid, lev_sum, int_id_found]; 

END; 
$$ LANGUAGE plpgsql; 


CREATE OR REPLACE FUNCTION translate_intersection_point(oid_geom GEOMETRY, metres FLOAT, direction TEXT)
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



CREATE OR REPLACE FUNCTION crosic.get_intersection_geom(highway2 TEXT, btwn TEXT, direction TEXT, metres FLOAT, not_int_id INT)
RETURNS TEXT[] AS $arr$
DECLARE 
geom TEXT;
int_arr INT[] := (CASE WHEN TRIM(highway2) = TRIM(btwn) THEN (crosic.get_intersection_id_highway_equals_btwn(highway2, btwn, not_int_id))
	ELSE (crosic.get_intersection_id(highway2, btwn, not_int_id))
	END);

oid INT := int_arr[1];
int_id_found INT := int_arr[3];
lev_sum INT := int_arr[2];
oid_geom geometry := (
		SELECT ST_Transform(ST_SetSRID(gis.geom, 4326), 26917)
		FROM gis.centreline_intersection gis
		WHERE objectid = oid
		);
arr1 TEXT[] :=  ARRAY(SELECT (
	-- normal case
	CASE WHEN direction IS NULL OR metres IS NULL 
	THEN ST_AsText(oid_geom)
	-- special case
	ELSE ST_AsText(translate_intersection_point(oid_geom, metres, direction))
	END 
));


arr2 TEXT[] := ARRAY_APPEND(arr1, int_id_found::TEXT);
arr TEXT[] := ARRAY_APPEND(arr2, lev_sum::TEXT);
BEGIN 

raise notice 'get_intersection_geom stuff: oid: % geom: % direction % metres % not_int_id: %', oid, ST_AsText(ST_Transform(oid_geom, 4326)), direction, metres::TEXT, not_int_id;

RETURN arr; 


END; 
$arr$ LANGUAGE plpgsql; 





DROP FUNCTION crosic.get_line_geom(oid1_geom geometry, oid2_geom geometry);
CREATE OR REPLACE FUNCTION crosic.get_line_geom(oid1_geom geometry, oid2_geom geometry)
RETURNS GEOMETRY AS $geom$
DECLARE 
len INT := ST_LENGTH(ST_MakeLine(oid1_geom, oid2_geom)); 
geom GEOMETRY := 
	(
	-- WARNING FOR LEN > 3000 ????????? 
	CASE WHEN len > 11 -- AND len < 20000
	THEN ST_Transform(ST_MakeLine(oid1_geom, oid2_geom), 26917)
	END
	);
BEGIN

raise notice 'LINE geom: % length of line: %', ST_AsText(ST_Transform(geom, 4326)), len;

RETURN geom;


END;
$geom$ LANGUAGE plpgsql; 


CREATE OR REPLACE FUNCTION crosic.match_line_to_centreline(line_geom geometry, highway2_before_editing text, metres_btwn1 FLOAT, metres_btwn2 FLOAT)
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
						SELECT DISTINCT lf_name,  levenshtein(LOWER(highway2), LOWER(s.lf_name), 1, 1, 2) as lev_dist
						FROM gis.centreline s
						WHERE levenshtein(LOWER(highway2), LOWER(s.lf_name), 1, 1, 2) < 3
						AND 
							(
							(
								-- "normal" case ... i.e. one intersection to another
								-- metres_btwn and metres_btwn2 are null 
								COALESCE(metres_btwn1, metres_btwn2) IS NULL AND 
								-- 10* used to be 3
								ST_DWithin( ST_Transform(s.geom, 26917), ST_BUFFER(line_geom, 3*ST_LENGTH(line_geom), 'endcap=flat join=round') , 10)
								AND ST_Length(st_intersection(ST_BUFFER(line_geom, 3*(ST_LENGTH(line_geom)), 'endcap=flat join=round') , ST_Transform(s.geom, 26917))) /ST_Length(ST_Transform(s.geom, 26917)) > 0.9 -- used to be 0.9
							)
							OR 
							(
								-- 10 TIMES LENGTH WORKS .... LOOK INTO POTENTIALLY CHANGING LATER
								COALESCE(metres_btwn1, metres_btwn2) IS NOT NULL AND
								ST_DWithin(ST_Transform(s.geom, 26917), ST_BUFFER(line_geom, 10*COALESCE(metres_btwn1, metres_btwn2), 'endcap=flat join=round'), 30)
							)
							)
						ORDER BY levenshtein(LOWER(highway2), LOWER(s.lf_name), 1, 1, 2)
						 ) AS x
				); 

street_name TEXT = street_name_arr[1];

geom geometry := (
	SELECT ST_Transform(ST_UNION(s.geom), 26917) -- ST_LineMerge(ST_Transform(ST_UNION(s.geom), 26917))
	FROM gis.centreline s
	WHERE lf_name = street_name
	AND 
	(
	(
		-- "normal" case ... i.e. one intersection to another
		-- metres_btwn and metres_btwn2 are null 
		COALESCE(metres_btwn1, metres_btwn2) IS NULL AND 
		-- 10* used to be 3
		ST_DWithin( ST_Transform(s.geom, 26917), ST_BUFFER(line_geom, 3*ST_LENGTH(line_geom), 'endcap=flat join=round') , 10)
		AND ST_Length(st_intersection(ST_BUFFER(line_geom, 3*(ST_LENGTH(line_geom)), 'endcap=flat join=round') , ST_Transform(s.geom, 26917))) /ST_Length(ST_Transform(s.geom, 26917)) > 0.9 -- used to be 0.9
	)
	OR 
	(
		-- 10 TIMES LENGTH WORKS .... LOOK INTO POTENTIALLY CHANGING LATER
		COALESCE(metres_btwn1, metres_btwn2) IS NOT NULL AND
		ST_DWithin(ST_Transform(s.geom, 26917), ST_BUFFER(line_geom, 10*COALESCE(metres_btwn1, metres_btwn2), 'endcap=flat join=round'), 30)
	)
	)
);

-- should always be less than 1
-- i.e. line_geom is shorter than geom
ratio NUMERIC := ST_Length(line_geom)/ST_Length(ST_LineMerge(geom));

BEGIN 

RAISE NOTICE 'centreline chunk of segments: %', ST_AsText(ST_Transform(ST_LineMerge(geom), 4326));

RETURN QUERY (SELECT geom, street_name_arr, ratio);
-- RETURN geom;

END;
$$ LANGUAGE plpgSQL; 





CREATE OR REPLACE FUNCTION crosic.centreline_case1(direction_btwn2 text, metres_btwn2 FLOAT, centreline_geom geometry, line_geom geometry, oid1_geom geometry)
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



-- get the closest line (when the line is a multi-linestring) to an intersection point
DROP FUNCTION crosic.get_closest_line(pnt GEOMETRY, multi_line GEOMETRY, line_geom GEOMETRY);

CREATE OR REPLACE FUNCTION crosic.get_closest_line(pnt GEOMETRY, multi_line GEOMETRY, line_geom GEOMETRY) 
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


-- function that is used to cut line segments for when the input is a multi-linestring
CREATE OR REPLACE FUNCTION crosic.cut_closest_line(direction_btwn1 TEXT, direction_btwn2 TEXT, metres_btwn1 FLOAT, metres_btwn2 FLOAT, centreline_geom_not_merged geometry, line_geom geometry, oid1_geom GEOMETRY, oid2_geom GEOMETRY) 
RETURNS geometry AS $geom$
DECLARE 

centreline_geom geometry := ST_LineMerge(centreline_geom_not_merged); 

closest_path_not_merged1 INT := (SELECT path FROM crosic.get_closest_line(oid1_geom, centreline_geom_not_merged, line_geom));

closest_line_geom1 GEOMETRY := (SELECT geom_part FROM crosic.get_closest_line(oid1_geom, centreline_geom, line_geom) );
				
closest_line_path1 INT := (SELECT path FROM crosic.get_closest_line(oid1_geom, centreline_geom, line_geom));
				
closest_line_geom2 GEOMETRY := (SELECT geom_part FROM crosic.get_closest_line(oid2_geom,centreline_geom, line_geom));
				
closest_line_path2 INT := (SELECT path FROM crosic.get_closest_line(oid2_geom,centreline_geom, line_geom));

closest_path_not_merged2 INT := (SELECT path FROM crosic.get_closest_line(oid2_geom,centreline_geom_not_merged, line_geom));

endpoint_line1 GEOMETRY := CASE WHEN closest_line_geom1 IS NULL THEN NULL
			WHEN ST_LineLocatePoint(closest_line_geom1, oid1_geom) < 0.5
			THEN ST_LineInterpolatePoint(closest_line_geom1, 0.99999)
			ELSE ST_LineInterpolatePoint(closest_line_geom1, 0.00001)
			END;

-- I DONT UNDERSTAND WHATS HAPPENING HERE
new_line1 GEOMETRY := (
			CASE WHEN direction_btwn1 IS NOT NULL AND direction_btwn2 IS NULL 
			THEN crosic.centreline_case2(NULL, direction_btwn1, NULL, metres_btwn1, closest_line_geom1, ST_MakeLine(oid1_geom, ST_SetSRID(translate_intersection_point(oid1_geom, metres_btwn1, direction_btwn1), 26917)), -- ST_MakeLine(oid1_geom, endpoint_line1), -- ST_MakeLine(oid1_geom, oid2_geom),
			oid1_geom, endpoint_line1)
			-- ??????????????? change this part below ???????
			WHEN direction_btwn1 IS NOT NULL AND direction_btwn2 IS NOT NULL 
			THEN crosic.centreline_case2(NULL, direction_btwn1, NULL, metres_btwn1, closest_line_geom1, ST_MakeLine(oid1_geom, ST_SetSRID(translate_intersection_point(oid1_geom, metres_btwn1, direction_btwn1), 26917)), -- ST_MakeLine(oid1_geom, oid2_geom), --ST_MakeLine(oid1_geom, endpoint_line1), 
			oid1_geom, endpoint_line1)
			-- from the point that oid1 intersects the merged centreline segment to the point where the merged centreline segments starts/ends
			ELSE crosic.line_substring_lower_value_first(closest_line_geom1, St_LineLocatePoint(closest_line_geom1, oid1_geom), ST_LineLocatePoint(closest_line_geom1, endpoint_line1))
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
			crosic.centreline_case2(direction_btwn2, NULL, metres_btwn2, NULL, closest_line_geom2, 
									ST_MakeLine(oid2_geom, ST_SetSRID(translate_intersection_point(oid2_geom, metres_btwn2, direction_btwn2), 26917)), -- ST_MakeLine(oid1_geom, endpoint_line1),   
									endpoint_line2, oid2_geom) 


			WHEN direction_btwn2 IS NOT NULL AND closest_line_path2 = closest_line_path1
			-- if the path is the same that means they are the same line segment 
			-- sooo just call case2 like normal on the one line segment
			-- this becomes a normal case 2 example, with closest_line_geom1 or 2 being the line 
			THEN crosic.centreline_case2(direction_btwn1, direction_btwn2, metres_btwn1, metres_btwn2, closest_line_geom1, line_geom, --ST_MakeLine(oid1_geom, endpoint_line1), 
			oid1_geom, oid2_geom)
			
			-- when direction_btwn2 is null and direction_btwn1 is not null 
			-- so newline1 is already a cut version of one of the lines 
			-- newline1 and this line will be created from different lines
			-- so the second line will be from the point that oid2 intersects the merged centreline segment to the point where the merged centreline segments starts/ends
			WHEN direction_btwn2 IS NULL AND closest_line_path2 <> closest_line_path1 
			THEN crosic.line_substring_lower_value_first(closest_line_geom2, St_LineLocatePoint(closest_line_geom2, oid2_geom), ST_LineLocatePoint(closest_line_geom2, endpoint_line2))


			-- when direction_btwn2 is null and direction_btwn1 is not null 
			-- newline1 and newline2 will be versions of the same line because 
			-- both oid1 and oid2 have the same line that is closest to them 
			-- so this line will be from oid2 to the end/start of newline1, S
			WHEN direction_btwn2 IS NULL AND closest_line_path2 = closest_line_path1 
			THEN crosic.line_substring_lower_value_first(new_line1, St_LineLocatePoint(new_line1, oid2_geom),ST_LineLocatePoint(new_line1, endpoint_line2))
			
			END );



final_line GEOMETRY :=  (
		SELECT ST_CollectionExtract(ST_Collect(geoms.geom), 2)
		FROM 
		(
			-- select all the original lines that were not replaced
			
			-- does not work/(doesnt do anything) because of line merge ... small lines being merged together and then cut
			--SELECT path[1], geom 
			--FROM St_Dump(centreline_geom) AS dump -- St_Dump(centreline_geom_not_merged) AS dump
			--WHERE  path[1] <> closest_line_path1 AND path[1] <> closest_line_path2 -- path[1] <> closest_path_not_merged1 AND path[1] <> closest_path_not_merged2
			--AND ST_INTERSECTS(dump.geom, new_line1) AND ST_INTERSECTS(dump.geom, new_line2)
			--AND -- buffer around newlines do not intersect with dump.geom ?????
			--ST_DWithin( ST_Transform(dump.geom, 26917), ST_BUFFER(ST_Transform(ST_MakeLine(oid1_geom, oid2_geom), 26917), ST_LENGTH(line_geom), 'endcap=flat join=round') , 1)
			--AND ST_Length(st_intersection(ST_BUFFER(ST_Transform(ST_MakeLine(oid1_geom, oid2_geom), 26917), (ST_LENGTH(ST_Transform(ST_MakeLine(oid1_geom, oid2_geom), 26917))), 'endcap=flat join=round') , ST_Transform(dump.geom, 26917))) /ST_Length(ST_Transform(dump.geom, 26917)) > 0.9
		
		--UNION 

			-- select the line that was replaced (i.e. the line closest to intersection 1/ oid1)
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




-- this function is used to make sure the lowest linelocatepoint fraction values comes first in the function call to st_linesubstring
-- in line substring lower fraction must come before a higher fraction 
CREATE OR REPLACE FUNCTION line_substring_lower_value_first(geom geometry, value1 float, value2 float)
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



CREATE OR REPLACE FUNCTION crosic.centreline_case2(direction_btwn1 text, direction_btwn2 text, metres_btwn1 FLOAT, metres_btwn2 FLOAT, centreline_geom_not_merged geometry, line_geom geometry, oid1_geom geometry, oid2_geom geometry)
RETURNS geometry AS $geom$
-- i.e. St Marks Ave and 100 metres north of St. John's Ave 

DECLARE 

centreline_geom geometry := ST_LineMerge(centreline_geom_not_merged);

geom geometry := (
	CASE 


	-- check if the geometry is a multi or single line part
	WHEN ST_GeometryType(centreline_geom) = 'ST_MultiLineString'
	THEN crosic.cut_closest_line(direction_btwn1, direction_btwn2, metres_btwn1, metres_btwn2, centreline_geom_not_merged, line_geom, oid1_geom, oid2_geom)


	
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
		line_substring_lower_value_first(centreline_geom, 
		ST_LineLocatePoint(centreline_geom, oid1_geom),  
		ST_LineLocatePoint(centreline_geom, oid2_geom) - (metres_btwn2/ST_Length(centreline_geom)))


		-- intersection of oid2 with centreline_geom has a smaller integer value than
		-- intersection with the end of the line_geom (i.e. the translated intersection of oid2) and centreline_geom
		-- since we wanna cut from a point that is very close to the end of line_geom, we need to add from oid2's linelocatepoint value 
	    -- think of it as we need to oid2 to be a value that is close to the linelocate value of translated oid2
		WHEN  ST_LineLocatePoint(centreline_geom, oid2_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		-- take the line from intersection 1 to x metres after intersection 2
			THEN 
			line_substring_lower_value_first(centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid1_geom),  
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom)))


		END
		)


	-- When direction 2 IS NULL 
	-- means that the zone is between intersection 2 and x metres away from intersection 1
	WHEN direction_btwn2 IS NULL

		THEN (
		-- intersection of oid1 with centreline_geom has a greater integer value than
		-- intersection with the beginning of the line_geom (i.e. the translated intersection of oid1) and centreline_geom
		-- since we wanna cut from a point that is very close to the beginning of line_geom, we need to subtract from oid1's linelocatepoint value 
	    -- think of it as we need to oid1 to be a value that is close to the linelocate value of translated oid1
		CASE WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0, 0.000001)))  --- ST_LineSubstring(line_geom, 0.99999, 1)))  --
		-- take the line from intersection 2 to x metres before intersection 1
			THEN 
			line_substring_lower_value_first(centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid1_geom) - (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom))

		-- intersection of oid1 with centreline_geom has a smaller integer value than
		-- intersection with the beginning of the line_geom (i.e. the translated intersection of oid1) and centreline_geom
		-- since we wanna cut from a point that is very close to the beginning of line_geom, we need to add from oid1's linelocatepoint value 
	    -- think of it as we need to oid1 to be a value that is close to the linelocate value of translated oid1
		WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0, 0.000001)))  --- ST_LineSubstring(line_geom, 0.99999, 1)))  --
			THEN 
			line_substring_lower_value_first(centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid1_geom) + (metres_btwn1/ST_Length(centreline_geom)), 
			ST_LineLocatePoint(centreline_geom, oid2_geom))

		END)


	-- when both are not null 
	-- the zone is between the point x metres away from int 1 and y metres away from int 2
	ELSE (
		-- both times the intersection occurs after (higher fraction) than the point closest to the end of the rough line 
		(CASE WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		AND  ST_LineLocatePoint(centreline_geom, oid1_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, line_geom))
			THEN 
			-- so line substring wont give error 
			line_substring_lower_value_first(centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid1_geom)- (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom)- (metres_btwn2/ST_Length(centreline_geom)))


		-- both before 
		WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		AND  ST_LineLocatePoint(centreline_geom, oid1_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, line_geom))

			THEN 
			line_substring_lower_value_first(centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid1_geom)+ (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom)))


		-- int 2 before (+), int 1 after (-)
		WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		AND  ST_LineLocatePoint(centreline_geom, oid1_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, line_geom))
			THEN 
			line_substring_lower_value_first(centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid1_geom)- (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom)+ (metres_btwn2/ST_Length(centreline_geom)))


		-- int 2 after(-), int 1 before (+)
		WHEN ST_LineLocatePoint(centreline_geom, oid2_geom)
		> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
		AND  ST_LineLocatePoint(centreline_geom, oid1_geom)
		< ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, line_geom))
			THEN 
			-- so line substring wont give error
			line_substring_lower_value_first(centreline_geom, 
			ST_LineLocatePoint(centreline_geom, oid1_geom) + (metres_btwn1/ST_Length(centreline_geom)),
			ST_LineLocatePoint(centreline_geom, oid2_geom) - (metres_btwn2/ST_Length(centreline_geom)))


		END
		) )



	END);



BEGIN 

raise notice 'IN THE CASE TWO FUNCTION !!!!!';
raise notice 'CASE 2 PARAMETERS  direction_btwn1 %, direction_btwn2 %, metres_btwn1 %, metres_btwn2 %, centreline_geom %, line_geom %, oid1_geom %, oid2_geom % output geom: %',direction_btwn1, direction_btwn2, metres_btwn1, metres_btwn2, ST_AsText(ST_Transform(centreline_geom, 4326)), ST_AsText(ST_Transform(line_geom, 4326)), ST_AsText(ST_Transform(oid1_geom, 4326)), ST_AsText(ST_Transform(oid2_geom, 4326)), ST_AsText(ST_Transform(geom, 4326));
--raise notice 'centreline line location of oid1_geom: % centreline location of line_goem: %, centreline line location of oid2_geom: %  centreline line location of end of line_geom %', ST_LineLocatePoint(centreline_geom, oid1_geom)::TEXT, ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, line_geom))::TEXT, ST_LineLocatePoint(centreline_geom, oid2_geom)::TEXT, ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))::TEXT;

RETURN geom;

END;
$geom$ LANGUAGE plpgSQL; 



CREATE OR REPLACE FUNCTION crosic.get_entire_length_centreline_segements(highway2_before_editing text)
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

geom GEOMETRY := CASE WHEN segments IS NOT NULL THEN ST_Transform(ST_LineMerge(segments),26917)
		ELSE NULL END;


BEGIN 

RETURN geom;

END;
$geom$ LANGUAGE plpgSQL; 



CREATE OR REPLACE FUNCTION crosic.text_to_centreline(highway TEXT, frm TEXT, t TEXT) 
RETURNS TABLE(centreline_segments geometry, con TEXT, street_name_arr TEXT[], ratio NUMERIC, notice TEXT, line_geom geometry, oid1_geom geometry, oid2_geom geometry) AS $$ 
DECLARE 


	-- clean data 

	
	-- when the input was btwn instead of from and to 
	
	btwn1_v1 TEXT := CASE WHEN t IS NULL THEN 
	gis.abbr_street2(regexp_REPLACE(regexp_REPLACE(regexp_REPLACE(split_part(split_part(regexp_REPLACE(regexp_REPLACE(frm, '[0123456789.,]* metres (north|south|east|west|East|northeast|northwest|southwest|southeast) of ', '', 'g'), '\(.*?\)', '', 'g'), ' to ', 1), ' and ', 1), '\(.*\)', '', 'g'), '[Bb]etween ', '', 'g'), 'A point', '', 'g'))
	ELSE gis.abbr_street2(regexp_REPLACE(regexp_REPLACE(regexp_REPLACE(frm, '\(.*?\)', '', 'g'), '[0123456789.,]* metres (north|south|east|west|East|northeast|northwest|southwest|southeast) of ', '', 'g'), 'A point', '', 'g')) 
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
			THEN gis.abbr_street2(regexp_REPLACE(regexp_REPLACE(split_part(regexp_REPLACE(regexp_REPLACE(regexp_REPLACE(frm, '\(.*?\)', '', 'g'), '[0123456789.,]* metres (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) of ', '', 'g'), 'the (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) end of', '', 'g'), ' and ', 2), '[Bb]etween ', '', 'g'), 'A point', '', 'g'))
			WHEN split_part(frm, ' to ', 2) <> ''
			THEN gis.abbr_street2(regexp_REPLACE(regexp_REPLACE(split_part(regexp_REPLACE(regexp_REPLACE(regexp_REPLACE(frm, '\(.*?\)', '', 'g'), '[0123456789.,]* metres (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) of ', '', 'g'), 'the (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) end of', '', 'g'), ' to ', 2), '[Bb]etween ', '', 'g'), 'A point', '', 'g'))
			END)
			
			ELSE 
			gis.abbr_street2(regexp_REPLACE(regexp_REPLACE(regexp_REPLACE(t, '\(.*?\)', '', 'g'), '[0123456789.]* metres (north|south|east|west|East|northeast|northwest|southwest|southeast|south west) of ', '', 'g'), 'the (north|south|east|west|East|east/north|northeast|northwest|southwest|southeast|south west) end of', '', 'g'))
			END ; 

	-- cases when three roads intersect at once (i.e. btwn2_orig_v1 = Terry Dr/Woolner Ave)
	btwn2_orig TEXT := (CASE WHEN btwn2_orig_v1 LIKE '%/%'
			THEN split_part(btwn2_orig_v1, '/', 1)
			ELSE btwn2_orig_v1
			END
	);
				
	highway2 TEXT :=  gis.abbr_street2(highway);
	
	direction_btwn1 TEXT := CASE WHEN t IS NULL THEN 
				(
				CASE WHEN btwn1 LIKE '% m %'
				OR gis.abbr_street2( regexp_REPLACE(split_part(split_part(frm, ' to ', 1), ' and ', 1), '[Bb]etween ', '', 'g')) LIKE '% m %'
				THEN split_part(split_part(gis.abbr_street2(regexp_REPLACE(regexp_REPLACE(regexp_REPLACE(split_part(split_part(frm, ' to ', 1), ' and ', 1), '[Bb]etween ', '', 'g'), 'further ', '', 'g'), 'east/north', 'northeast', 'g')), ' m ', 2), ' of ', 1)
				ELSE NULL
				END )
				ELSE 
				(
				CASE WHEN btwn1 LIKE '% m %'
				OR gis.abbr_street2(frm) LIKE '% m %'
				THEN regexp_replace(regexp_replace(split_part(split_part(gis.abbr_street2(frm), ' m ', 2), ' of ', 1), 'further ', '', 'g'), 'east/north', 'northeast', 'g')
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
				OR gis.abbr_street2(regexp_REPLACE(split_part(split_part(frm, ' to ', 1), ' and ', 1), 'Between ', '', 'g')) LIKE '% m %'
				THEN regexp_REPLACE(regexp_REPLACE(regexp_REPLACE(split_part(gis.abbr_street2(regexp_REPLACE(split_part(split_part(frm, ' to ', 1), ' and ', 1), '[Bb]etween ', '', 'g')), ' m ' ,1), 'a point ', '', 'g'), 'A point', '', 'g'), ',', '', 'g')::FLOAT
				ELSE NULL
				END
				)
				ELSE 
				(
				CASE WHEN btwn1 LIKE '% m %'
				OR gis.abbr_street2(frm) LIKE '% m %'
				THEN regexp_REPLACE(regexp_REPLACE(regexp_REPLACE(split_part(gis.abbr_street2(frm), ' m ' ,1), 'a point ', '', 'g'), 'A point', '', 'g'), ',', 'g')::FLOAT
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



	-- for case one 
	-- i.e. Watson road from St. Mark's Road to a point 100 metres north
	-- we want the btwn2 to be St. Mark's Road (which is also btwn1)
	btwn2 TEXT := (
	CASE WHEN btwn2_orig LIKE '%point%' AND (btwn2_check NOT LIKE '% of %' OR btwn2_check LIKE ('% of ' || TRIM(btwn1)))
	THEN TRIM(gis.abbr_street2(btwn1))
	ELSE TRIM(gis.abbr_street2(regexp_replace(btwn2_orig , 'a point', '', 'g')))
	END
	);






	-- get intersection geoms

	text_arr_oid1 TEXT[]:= crosic.get_intersection_geom(highway2, btwn1, direction_btwn1::TEXT, metres_btwn1::FLOAT, 0);

	-- used to keep track of the ID of the first intersection so we don't assign the same intersection twice
	int_id1 INT := (text_arr_oid1[2])::INT;
	
	oid1_geom GEOMETRY := ST_GeomFromText(text_arr_oid1[1], 26917);

	text_arr_oid2 TEXT[] := (CASE WHEN btwn2_orig LIKE '%point%' AND (btwn2_check NOT LIKE '% of %' OR btwn2_check LIKE ('% of ' || TRIM(btwn1)))
				THEN crosic.get_intersection_geom(highway2, btwn2, direction_btwn2::TEXT, metres_btwn2::FLOAT, 0)
				ELSE crosic.get_intersection_geom(highway2, btwn2, direction_btwn2::TEXT, metres_btwn2::FLOAT, int_id1)
				END);

	oid2_geom  GEOMETRY := ST_GeomFromText(text_arr_oid2[1], 26917);


	-- create a line between the two intersection geoms
	line_geom geometry := (CASE WHEN oid1_geom IS NOT NULL AND oid2_geom IS NOT NULL 
						  THEN crosic.get_line_geom(oid1_geom, oid2_geom)
						   ELSE NULL END);

	match_line_to_centreline_geom geometry := (SELECT geom FROM crosic.match_line_to_centreline(line_geom, highway2, metres_btwn1, metres_btwn2));
	
	match_line_to_centreline_ratio NUMERIC;
	
	match_line_to_centreline_street_name_arr TEXT[]; 

	-- match the lines to centreline segments
	centreline_segments geometry := (
				CASE WHEN (TRIM(btwn1) = 'Entire length' OR TRIM(btwn1) ='Entire Length' OR TRIM(btwn1) = 'entire length' OR TRIM(btwn1) = 'The entire length') AND btwn2 IS NULL 
				THEN (SELECT * FROM get_entire_length_centreline_segements(highway2) LIMIT 1)
		
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
					crosic.centreline_case1(direction_btwn2, metres_btwn2, ST_MakeLine(ST_LineMerge(match_line_to_centreline_geom)), line_geom, 
					ST_GeomFromText((crosic.get_intersection_geom(highway2, btwn1, NULL::TEXT, NULL::FLOAT, 0))[1], 26917) )
				)

				-- special case 2
				ELSE 
				(
					crosic.centreline_case2(direction_btwn1, direction_btwn2, metres_btwn1, metres_btwn2, match_line_to_centreline_geom, line_geom, 
					-- get the original intersection geoms (not the translated ones) 
					ST_GeomFromText((crosic.get_intersection_geom(highway2, btwn1, NULL::TEXT, NULL::FLOAT, 0))[1], 26917),(CASE WHEN btwn2_orig LIKE '%point%' AND (btwn2_check NOT LIKE '% of %' OR btwn2_check LIKE ('% of ' || TRIM(btwn1)))
					THEN ST_GeomFromText((crosic.get_intersection_geom(highway2, btwn2, NULL::TEXT, NULL::FLOAT, 0))[1], 26917)
					ELSE ST_GeomFromText((crosic.get_intersection_geom(highway2, btwn2, NULL::TEXT, NULL::FLOAT, int_id1))[1], 26917)
					END))
				)
				END
				);


	-- sum of the levenshtein distance of both of the intersections matched
	lev_sum INT := text_arr_oid1[3]::INT + text_arr_oid2[3]::INT; -- (crosic.get_intersection_id(highway2, btwn1))[2] + (crosic.get_intersection_id(highway2, btwn2))[2];

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
FROM crosic.match_line_to_centreline(line_geom, highway2, metres_btwn1, metres_btwn2) AS tbl;

raise notice 'btwn1: % btwn2: % btwn2_check: %  highway2: % metres_btwn1: %  metres_btwn2: % direction_btwn1: % direction_btwn2: % cntreline_segments: %', btwn1, btwn2, btwn2_check, highway2, metres_btwn1, metres_btwn2, direction_btwn1, direction_btwn2, ST_ASText(ST_Transform(centreline_segments, 4326));
	

RETURN QUERY (SELECT centreline_segments, con, match_line_to_centreline_street_name_arr AS street_name_arr, match_line_to_centreline_ratio AS ratio, notice, line_geom, oid1_geom, oid2_geom);


END;
$$ LANGUAGE plpgsql; 






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
SELECT i.highway, i.frm AS from, i.t AS to, (SELECT con FROM crosic.text_to_centreline(i.highway, i.frm, i.t)) AS confidence, 
(SELECT centreline_segments FROM crosic.text_to_centreline(i.highway, i.frm, i.t)) AS geom

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




SELECT * FROM crosic.centreline_geoms_test;