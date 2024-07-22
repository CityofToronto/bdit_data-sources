CREATE OR REPLACE FUNCTION gis.bylaws_get_id_to_route(
    _bylaw_id integer,
    highway text,
    frm text,
    t text
)
RETURNS TABLE (
    bylaw_id integer, note text, highway2 text, int1 integer, int2 integer,
    oid1_geom geometry,
    oid1_geom_translated geometry,
    oid2_geom geometry,
    oid2_geom_translated geometry,
    lev_sum1 integer, lev_sum2 integer
)
LANGUAGE 'plpgsql'

COST 100
VOLATILE
ROWS 1000
AS $BODY$

DECLARE

clean_bylaws RECORD;
int1_result RECORD;
int2_result RECORD;
note text;
    
BEGIN

--STEP 1 
    -- clean bylaws text
    clean_bylaws := gis._clean_bylaws_text(_bylaw_id, highway, frm, t);

--STEP 2
    -- get centrelines geoms
    CREATE TEMP TABLE IF NOT EXISTS _results(
        int_start int,
        int_end int,
        oid1_geom geometry,
        oid1_geom_translated geometry,
        oid2_geom geometry,
        oid2_geom_translated geometry,
        lev_sum1 int,
        lev_sum2 int
    );

int1_result := gis._get_intersection_geom(clean_bylaws.highway2, clean_bylaws.btwn1, clean_bylaws.direction_btwn1, clean_bylaws.metres_btwn1, 0);

int2_result := (CASE WHEN clean_bylaws.btwn2_orig LIKE '%point%' AND (clean_bylaws.btwn2_check NOT LIKE '% of %' OR clean_bylaws.btwn2_check LIKE ('% of ' || TRIM(clean_bylaws.btwn1)))
            THEN gis._get_intersection_geom(clean_bylaws.highway2, clean_bylaws.btwn2, clean_bylaws.direction_btwn2, clean_bylaws.metres_btwn2, 0)
            ELSE gis._get_intersection_geom(clean_bylaws.highway2, clean_bylaws.btwn2, clean_bylaws.direction_btwn2, clean_bylaws.metres_btwn2, int1_result.int_id_found)
            END);

INSERT INTO _results(int_start, int_end,
oid1_geom, oid1_geom_translated, oid2_geom, oid2_geom_translated, lev_sum1, lev_sum2)
SELECT int1_result.int_id_found, int2_result.int_id_found, 
int1_result.oid_geom AS oid1_geom, int1_result.oid_geom_translated AS oid1_geom_translated,
int2_result.oid_geom AS oid2_geom, int2_result.oid_geom_translated AS oid2_geom_translated,
int1_result.lev_sum, int2_result.lev_sum;

note := format('highway2: %s btwn1: %s btwn2: %s metres_btwn1: %s metres_btwn2: %s direction_btwn1: %s direction_btwn2: %s', 
    clean_bylaws.highway2, clean_bylaws.btwn1, clean_bylaws.btwn2, clean_bylaws.metres_btwn1, clean_bylaws.metres_btwn2, 
    clean_bylaws.direction_btwn1, clean_bylaws.direction_btwn2);    
    
RETURN QUERY 
SELECT clean_bylaws.bylaw_id, note, clean_bylaws.highway2, int_start, int_end, 
r.oid1_geom, r.oid1_geom_translated, r.oid2_geom, r.oid2_geom_translated, 
r.lev_sum1, r.lev_sum2
FROM _results r;

DROP TABLE _results;

END;
$BODY$;
