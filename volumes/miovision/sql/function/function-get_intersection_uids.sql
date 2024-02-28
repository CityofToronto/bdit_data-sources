CREATE OR REPLACE FUNCTION miovision_api.get_intersections_uids(
    intersections integer [] DEFAULT ARRAY[]::integer []
)
RETURNS integer[]
LANGUAGE plpgsql
VOLATILE
COST 100

AS $BODY$

BEGIN
    RETURN
        CASE WHEN CARDINALITY(intersections) = 0
            --switch out a blank array for all intersections
            THEN (SELECT ARRAY_AGG(i.intersection_uid) FROM miovision_api.intersections AS i)
            ELSE intersections
        END;
END;

$BODY$;

COMMENT ON FUNCTION miovision_api.get_intersections_uids(integer []) IS
'''Returns all intersection_uids if optional `intersections` param is omitted, otherwise
returns only the intersection_uids provided as an integer array to intersections param.
Used in `miovision_api.clear_*` functions.
Example usage:
`SELECT miovision_api.get_intersections_uids() --returns all intersection_uids` or
`SELECT miovision_api.get_intersections_uids(ARRAY[1,2,3]::integer[]) --returns only {1,2,3}`''';