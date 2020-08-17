CREATE OR REPLACE FUNCTION jchew._get_intersection_id_updated(
	highway2 text,
	btwn text,
	not_int_id integer)
    RETURNS integer[]
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE
oid INT;
lev_sum INT;
int_id_found INT;

BEGIN

SELECT intersections.objectid, 
--sum up the least levenshtein distance for each interxn & highway2 and interxn & btwn
SUM(LEAST(levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1), levenshtein(TRIM(intersections.street), TRIM(btwn), 1, 1, 1))), 
intersections.int_id
INTO oid, lev_sum, int_id_found
FROM
(gis.centreline_intersection_streets LEFT JOIN gis.centreline_intersection USING(objectid)) AS intersections

--To filter intersections that are matched to `highway2` or `btwn` streetnames										 
WHERE (levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1) < 4 
OR levenshtein(TRIM(intersections.street), TRIM(btwn), 1, 1, 1) < 4) 
AND intersections.int_id  <> not_int_id

GROUP BY intersections.objectid, intersections.int_id
--to ensure that the intersection is matched to both `highway2` and `btwn` streetnames as some intersections might have three streets like Lawrence Ave W/Yonge St/Lawrence Ave E
HAVING COUNT(DISTINCT (CASE WHEN levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1) <  levenshtein(TRIM(intersections.street), TRIM(btwn), 1, 1, 1) THEN highway2 ELSE btwn END) ) > 1
ORDER BY AVG(LEAST(levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1), levenshtein(TRIM(intersections.street),  TRIM(btwn), 1, 1, 1)))

LIMIT 1;

RAISE NOTICE 'highway2 being matched: % btwn being matched: % not_int_id: % intersection arr: %', highway2, btwn, not_int_id, ARRAY[oid, lev_sum];

RETURN ARRAY[oid, lev_sum, int_id_found];

END;
$BODY$;

COMMENT ON FUNCTION jchew._get_intersection_id_updated(text, text, integer) IS '
Input two street names of streets that intersect each other, and 0 or an intersection id that you do not want the function to return
(i.e. sometimes two streets intersect each other twice so if you want to get both intersections by calling this function you would input the first returned intersection id
into the function on the second time the function is called).
This function returns the objectid and intersection id of the intersection, as well as how close the match was. Closeness is measued by levenshtein distance.';


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
ORDER BY AVG (LEAST(levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1)
, levenshtein(TRIM(intersections.street),  TRIM(btwn), 1, 1, 1))),
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

--CHANGED, Modified the function so that the translation is happening at an angle 17.5 like the Toronto map
CREATE OR REPLACE FUNCTION jchew._translate_intersection_point_updated(oid_geom GEOMETRY, metres FLOAT, direction TEXT)
RETURNS GEOMETRY AS $translated_geom$
DECLARE
translated_geom TEXT := (
	(CASE WHEN TRIM(direction) = 'west' THEN ST_Translate(oid_geom, -metres, 0)
	WHEN TRIM(direction) = 'east' THEN ST_Translate(oid_geom, metres, 0)
	WHEN TRIM(direction) = 'north' THEN ST_Translate(oid_geom, 0, metres)
	WHEN TRIM(direction) = 'south' THEN ST_Translate(oid_geom, 0, -metres)
	WHEN TRIM(direction) IN ('southwest', 'south west') THEN ST_Translate(oid_geom, -cos(45)*metres, -sin(45)*metres)
	WHEN TRIM(direction) IN ('southeast', 'south east') THEN ST_Translate(oid_geom, cos(45)*metres, -sin(45)*metres)
	WHEN TRIM(direction) IN ('northwest', 'north west') THEN ST_Translate(oid_geom, -cos(45)*metres, sin(45)*metres)
	WHEN TRIM(direction) IN ('northeast', 'north east') THEN ST_Translate(oid_geom, cos(45)*metres, sin(45)*metres)
	END)
	);

BEGIN
--rotate every point to 17.5 degree anticlockwise (0.305 radians) based on the intersection point
RETURN ST_Transform(ST_SetSRID(ST_Rotate(translated_geom, 0.305, oid_geom), 2952), 4326);

END;
$translated_geom$ LANGUAGE plpgsql;


COMMENT ON FUNCTION jchew._translate_intersection_point_updated(oid_geom GEOMETRY, metres FLOAT, direction TEXT) IS '
Inputs are the geometry of a point, the number of units that you would like the point to be translated, and the direction that you would like the point to be translated.
The function returns the input point geometry translated in the direction inputted, by the number of metres inputted.
Note: if you would like to move your geometry by a certian number of metres, make sure the point geometry has a projection that has metres as the unit.
If the input geometry is unprojected (i.e. SRID = 4326) then the function returns a geometry that has been translated by the number of degrees inputted.
';

--CHANGED
DROP FUNCTION jchew._get_intersection_geom_updated(text, text, text, double precision, integer);
CREATE OR REPLACE FUNCTION jchew._get_intersection_geom_updated(highway2 TEXT, btwn TEXT, direction TEXT, metres FLOAT, not_int_id INT,
OUT oid_geom GEOMETRY, OUT oid_geom_translated GEOMETRY, OUT int_id_found INT, OUT lev_sum INT)

LANGUAGE 'plpgsql'
AS $BODY$

DECLARE
geom TEXT;
int_arr INT[];
oid_int INT;
oid_geom_test GEOMETRY;

BEGIN
int_arr := (CASE WHEN TRIM(highway2) = TRIM(btwn) 
	THEN (gis._get_intersection_id_highway_equals_btwn(highway2, btwn, not_int_id))
	ELSE (jchew._get_intersection_id_updated(highway2, btwn, not_int_id))
	END);

oid_int := int_arr[1];
int_id_found := int_arr[3];
lev_sum := int_arr[2];

--needed geom to be in SRID = 2952 for the translation
oid_geom_test := (
		SELECT ST_Transform(ST_SetSRID(gis.geom, 4326), 2952)
		FROM gis.centreline_intersection gis
		WHERE objectid = oid_int
		);
oid_geom_translated := (
		CASE WHEN direction IS NOT NULL OR metres IS NOT NULL
	   	THEN (SELECT *
           FROM jchew._translate_intersection_point_updated(oid_geom_test, metres, direction) translated_geom)
		ELSE NULL
		END
		);
oid_geom := (
		SELECT gis.geom 
		FROM gis.centreline_intersection gis
		WHERE objectid = oid_int
		);

RAISE NOTICE 'get_intersection_geom stuff: oid: % geom: % geom_translated: % direction % metres % not_int_id: %', 
oid_int, ST_AsText(oid_geom), ST_AsText(oid_geom_translated), direction, metres::TEXT, not_int_id;

END;
$BODY$;

COMMENT ON FUNCTION jchew._get_intersection_geom_updated(TEXT, TEXT, TEXT, FLOAT, INT) IS '
Input values of the names of two intersections, direction (may be NULL), number of units the intersection should be translated,
and the intersection id of an intersection that you do not want the function to return (or 0).

Returns a record of oid_geom, oid_geom_translated (if applicable lese null), int_id_found and lev_sum. 
Outputs an array of the geometry of the intersection described. 
If the direction and metres are not null, it will return the point geometry (known as oid_geom_translated),
translated by x metres in the inputted direction. It also returns (in the array) the intersection id and the objectid of the output intersection.
Additionally, the levenshein distance between the text inputs described and the output intersection name is in the output array.
';
