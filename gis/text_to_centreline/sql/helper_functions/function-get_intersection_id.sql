CREATE OR REPLACE FUNCTION gwolofs._get_intersection_id(
    highway2 text, btwn text, not_int_id int
)
RETURNS int [] AS $$

DECLARE
oid int;
lev_sum int;
int_id_found int;

BEGIN

WITH intersections AS (
    SELECT DISTINCT
        objectid,
        trim(unnest(string_to_array(intersection_desc::text, '/'::text))) AS street,
        intersection_id AS int_id,
        levenshtein(TRIM(intersections.street), TRIM(highway2), 1, 1, 1) AS levenshtein_1,
        levenshtein(TRIM(intersections.street), TRIM(btwn), 1, 1, 1) AS levenshtein_2
    FROM gis_core.centreline_intersection_point_latest
)

SELECT
    intersections.objectid, 
    SUM(LEAST(levenshtein_1, levenshtein_2)),
    intersections.int_id
INTO oid, lev_sum, int_id_found
FROM intersections
WHERE
    (levenshtein_1 < 4  OR levenshtein_2 < 4) 
    --AND intersections.int_id <> not_int_id
GROUP BY
    intersections.objectid,
    intersections.int_id
HAVING COUNT(DISTINCT TRIM(intersections.street)) > 1
ORDER BY AVG(LEAST(levenshtein_1, levenshtein_2))
LIMIT 1;

RAISE NOTICE 'highway2 being matched: %, btwn being matched: %, not_int_id: %, intersection arr: %', highway2, btwn, not_int_id, ARRAY[oid, lev_sum];

RETURN ARRAY[oid, lev_sum, int_id_found];

END;
$$ LANGUAGE plpgsql;


COMMENT ON FUNCTION gwolofs._get_intersection_id(text, text, int) IS '
Input two street names of streets that intersect each other, and 0 or an intersection id that you do not want the function to return
(i.e. sometimes two streets intersect each other twice so if you want to get both intersections by calling this function you would input the first returned intersection id
into the function on the second time the function is called).
This function returns the objectid and intersection id of the intersection, as well as how close the match was. Closeness is measued by levenshtein distance.';
