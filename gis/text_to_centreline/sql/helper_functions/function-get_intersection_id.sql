CREATE OR REPLACE FUNCTION gis._get_intersection_id(
    highway2 TEXT, btwn TEXT, not_int_id INT
)
RETURNS INT[] AS $$
DECLARE
oid INT;
lev_sum INT;
int_id_found INT;

BEGIN
SELECT intersections.objectid, 

SUM(LEAST
(levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1)
, levenshtein(TRIM(intersections.street), TRIM(btwn), 1, 1, 1)))

, intersections.int_id
INTO oid, lev_sum, int_id_found
FROM
(gis.centreline_intersection_streets --to get street name
LEFT JOIN gis.centreline_intersection --to get int_id 
USING(objectid)) AS intersections


WHERE (levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1) < 4 
OR levenshtein(TRIM(intersections.street), TRIM(btwn), 1, 1, 1) < 4) 
AND intersections.int_id  <> not_int_id


GROUP BY intersections.objectid, intersections.int_id
HAVING COUNT(DISTINCT TRIM(intersections.street)) > 1
ORDER BY AVG(LEAST(levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1), levenshtein(TRIM(intersections.street),  TRIM(btwn), 1, 1, 1)))

LIMIT 1;

RAISE NOTICE 'highway2 being matched: %, btwn being matched: %, not_int_id: %, intersection arr: %', highway2, btwn, not_int_id, ARRAY[oid, lev_sum];

RETURN ARRAY[oid, lev_sum, int_id_found];

END;
$$ LANGUAGE plpgsql;


COMMENT ON FUNCTION gis._get_intersection_id(TEXT, TEXT, INT) IS '
Input two street names of streets that intersect each other, and 0 or an intersection id that you do not want the function to return
(i.e. sometimes two streets intersect each other twice so if you want to get both intersections by calling this function you would input the first returned intersection id
into the function on the second time the function is called).
This function returns the objectid and intersection id of the intersection, as well as how close the match was. Closeness is measued by levenshtein distance.';
