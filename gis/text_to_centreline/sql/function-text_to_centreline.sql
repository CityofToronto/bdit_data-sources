CREATE OR REPLACE FUNCTION gwolofs.text_to_centreline(
    _bylaw_id integer,
    highway text,
    frm text,
    t text
)
RETURNS TABLE (
    int1 integer,
    int2 integer,
    geo_id numeric,
    lf_name character varying,
    con text,
    note text,
    line_geom geometry,
    section numrange,
    oid1_geom geometry,
    oid1_geom_translated geometry,
    oid2_geom geometry,
    oid2_geom_translated geometry,
    objectid numeric,
    fcode integer,
    fcode_desc character varying
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
    lev_total int;
    con text;
    note text;
    text_var1 text;
    text_var2 text;
    text_var3 text;

BEGIN 
--STEP 1 
    -- clean bylaws text
    clean_bylaws := gwolofs._clean_bylaws_text(
        _bylaw_id := text_to_centreline._bylaw_id,
        highway := initcap(text_to_centreline.highway),
        frm := initcap(text_to_centreline.frm),
        t := initcap(text_to_centreline.t)
    );

--STEP 2
    -- get centrelines geoms
    CREATE TEMP TABLE IF NOT EXISTS _results(
        int_start int,
        int_end int,
        seq int,
        geo_id numeric,
        lf_name varchar,
        line_geom geometry,
        section NUMRANGE,
        oid1_geom geometry,
        oid1_geom_translated geometry,
        oid2_geom geometry,
        oid2_geom_translated geometry,
        objectid numeric,
        fcode int,
        fcode_desc varchar,
        lev_sum int
    );

    TRUNCATE TABLE _results;

    --entire length cases
    IF TRIM(clean_bylaws.btwn1) ILIKE '%entire length%' AND clean_bylaws.btwn2 IS NULL
        THEN
        INSERT INTO _results(geo_id, lf_name, objectid, line_geom, fcode, fcode_desc)
        SELECT centreline_id, linear_name_full, objectid, geom, feature_code, feature_code_desc
        FROM gwolofs._get_entire_length(clean_bylaws.highway2);
        --lev_total := NULL

    --normal cases
    ELSIF COALESCE(clean_bylaws.metres_btwn1, clean_bylaws.metres_btwn2) IS NULL
        THEN
        int1_result := gwolofs._get_intersection_geom(clean_bylaws.highway2, clean_bylaws.btwn1, clean_bylaws.direction_btwn1, clean_bylaws.metres_btwn1, 0);

        int2_result := (CASE WHEN clean_bylaws.btwn2_orig ILIKE '%point%' AND (clean_bylaws.btwn2_check NOT ILIKE '% of %' OR clean_bylaws.btwn2_check ILIKE ('% of ' || TRIM(clean_bylaws.btwn1)))
                    THEN gwolofs._get_intersection_geom(clean_bylaws.highway2, clean_bylaws.btwn2, clean_bylaws.direction_btwn2, clean_bylaws.metres_btwn2, 0)
                    ELSE gwolofs._get_intersection_geom(clean_bylaws.highway2, clean_bylaws.btwn2, clean_bylaws.direction_btwn2, clean_bylaws.metres_btwn2, int1_result.int_id_found)
                    END);
                    
        INSERT INTO _results(int_start, int_end, seq, geo_id, lf_name, line_geom,
        oid1_geom, oid1_geom_translated, oid2_geom, oid2_geom_translated, objectid,    fcode, fcode_desc)
        SELECT int_start, int_end, seq, rout.geo_id, rout.lf_name, geom AS line_geom, 
        int1_result.oid_geom AS oid1_geom, int1_result.oid_geom_translated AS oid1_geom_translated,
        int2_result.oid_geom AS oid2_geom, int2_result.oid_geom_translated AS oid2_geom_translated,
        rout.objectid, rout.fcode, rout.fcode_desc
        FROM gwolofs._get_lines_btwn_interxn(clean_bylaws.highway2, int1_result.int_id_found, int2_result.int_id_found) rout;

        -- sum of the levenshtein distance of both of the intersections matched
        UPDATE _results SET lev_sum = int1_result.lev_sum + int2_result.lev_sum;
    
    --interxn_and_offset
    ELSIF clean_bylaws.btwn1 = clean_bylaws.btwn2
        THEN
        INSERT INTO _results(int_start, geo_id, lf_name, line_geom, section, oid1_geom, oid1_geom_translated, objectid, fcode, fcode_desc, lev_sum)
        SELECT case1.int1, case1.geo_id, case1.lf_name, case1.line_geom, case1.section, 
        case1.oid1_geom, case1.oid1_geom_translated, case1.objectid, case1.fcode, case1.fcode_desc, case1.lev_sum
        FROM gwolofs._centreline_case1(clean_bylaws.highway2, clean_bylaws.btwn2, clean_bylaws.direction_btwn2, clean_bylaws.metres_btwn2) case1;
    
    --interxns_and_offsets
    ELSE 
        INSERT INTO _results(int_start, int_end, seq, geo_id, lf_name, line_geom, section,
        oid1_geom, oid1_geom_translated, oid2_geom, oid2_geom_translated, objectid, fcode, fcode_desc, lev_sum)
        SELECT case2.int_start, case2.int_end, case2.seq, case2.geo_id, case2.lf_name, case2.line_geom, case2.section, 
        case2.oid1_geom, case2.oid1_geom_translated, case2.oid2_geom, case2.oid2_geom_translated, 
        case2.objectid, case2.fcode, case2.fcode_desc, case2.lev_sum
        FROM gwolofs._centreline_case2(clean_bylaws.highway2, clean_bylaws.btwn1, clean_bylaws.direction_btwn1, clean_bylaws.metres_btwn1,
        clean_bylaws.btwn2, clean_bylaws.direction_btwn2, clean_bylaws.metres_btwn2, clean_bylaws.btwn2_orig, clean_bylaws.btwn2_check) case2 ;

    END IF;

    lev_total := AVG(_results.lev_sum) FROM _results GROUP BY _results.lf_name LIMIT 1;

    -- confidence value
    con := (
        CASE WHEN TRIM(clean_bylaws.btwn1) ILIKE '%entire length%' AND clean_bylaws.btwn2 IS NULL
        THEN 'Maybe (Entire length of road)'
        WHEN lev_total IS NULL
        THEN 'No Match'
        WHEN lev_total = 0
        THEN 'Very High (100% match)'
        WHEN lev_total = 1
        THEN 'High (1 character difference)'
        WHEN lev_total IN (2,3)
        THEN FORMAT('Medium (%s character difference)', lev_total::text)
        ELSE FORMAT('Low (%s character difference)', lev_total::text)
        END
    );

    note := format('highway2: %s btwn1: %s btwn2: %s metres_btwn1: %s metres_btwn2: %s direction_btwn1: %s direction_btwn2: %s', 
    clean_bylaws.highway2, clean_bylaws.btwn1, clean_bylaws.btwn2, clean_bylaws.metres_btwn1, clean_bylaws.metres_btwn2, 
    clean_bylaws.direction_btwn1, clean_bylaws.direction_btwn2);    

RAISE NOTICE 'highway2: %, btwn1: %, btwn2: %, btwn2_check: %, metres_btwn1: %, metres_btwn2: %, direction_btwn1: %, direction_btwn2: %', 
clean_bylaws.highway2, clean_bylaws.btwn1, clean_bylaws.btwn2, clean_bylaws.btwn2_check, 
clean_bylaws.metres_btwn1, clean_bylaws.metres_btwn2, clean_bylaws.direction_btwn1, clean_bylaws.direction_btwn2;

RETURN QUERY 
SELECT int_start, int_end, r.geo_id, r.lf_name, con, note, 
r.line_geom, r.section, r.oid1_geom, r.oid1_geom_translated, 
r.oid2_geom, r.oid2_geom_translated, 
r.objectid, r.fcode, r.fcode_desc 
FROM _results r;

DROP TABLE _results;

EXCEPTION WHEN SQLSTATE 'XX000' THEN
    RAISE WARNING 'Internal error at mega for bylaw_id = % : ''%'' ', _bylaw_id, SQLERRM ;

END;
$BODY$;

ALTER FUNCTION gwolofs.text_to_centreline(integer, text, text, text)
OWNER TO gwolofs;

GRANT EXECUTE ON FUNCTION gwolofs.text_to_centreline(
    integer, text, text, text
) TO bdit_humans;

GRANT EXECUTE ON FUNCTION gwolofs.text_to_centreline(
    integer, text, text, text
) TO gwolofs;

REVOKE ALL ON FUNCTION gwolofs.text_to_centreline(
    integer, text, text, text
) FROM public;

COMMENT ON FUNCTION gwolofs.text_to_centreline(integer, text, text, text)
IS '
The main function for converting text descriptions of locations where bylaws are in effect to centreline segment geometry
Check out README in https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/text_to_centreline for more information
';
