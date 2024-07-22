-- get intersection id when intersection is a cul de sac or a dead end or a pseudo intersection
-- in these cases the intersection would just be the name of the street
-- some cases have a road that starts and ends in a cul de sac, so not_int_id is the intersection_id of the
-- first intersection in the bylaw text (or 0 if we are trying to find the first intersection).
-- not_int_id is there so we dont ever get the same intersections matched twice
CREATE OR REPLACE FUNCTION gis._get_intersection_id_highway_equals_btwn(
    highway2 TEXT, btwn TEXT, not_int_id INT
)
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

RAISE NOTICE '(highway=btwn) highway2 being matched: %, btwn being matched: %, not_int_id: %, intersection arr: %', highway2, btwn, not_int_id, ARRAY[oid, lev_sum];

RETURN ARRAY[oid, lev_sum, int_id_found];

END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION gis._get_intersection_id_highway_equals_btwn(
    TEXT, TEXT, INT
) IS '
Get intersection id from a text street name when intersection is a cul de sac or a dead end or a pseudo intersection.
In these cases the intersection name (intersec5 of gis.centreline_intersection) would just be the name of the street.

Input two street names of streets that intersect each other, and 0 or an intersection id that you do not want the function to return
(i.e. sometimes two streets intersect each other twice so if you want to get both intersections by calling this function you would input the
first returned intersection id into the function on the second time the function is called).
This function returns the objectid and intersection id of the intersection, as well as how close the match was. Closeness is measued by levenshtein distance.';
