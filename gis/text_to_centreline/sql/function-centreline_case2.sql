DROP FUNCTION gis._centreline_case2 (
    text, text, text, double precision, text, text, double precision, text, text
);
CREATE OR REPLACE FUNCTION gis._centreline_case2(
    highway2 text,
    btwn1 text,
    direction_btwn1 text,
    metres_btwn1 double precision,
    btwn2 text,
    direction_btwn2 text,
    metres_btwn2 double precision,
    btwn2_orig text,
    btwn2_check text
)
RETURNS TABLE (
    int_start integer,
    int_end integer,
    seq integer,
    geo_id numeric,
    lf_name character varying,
    ind_line_geom geometry, line_geom geometry, line_geom_cut geometry,
    section numrange,
    oid1_geom geometry,
    oid1_geom_translated geometry,
    oid2_geom geometry,
    oid2_geom_translated geometry,
    objectid numeric,
    fcode integer,
    fcode_desc character varying,
    lev_sum integer
)
LANGUAGE 'plpgsql'

COST 100
VOLATILE
ROWS 1000
AS $BODY$

DECLARE
_int1_result record;
_int2_result record;

BEGIN

CREATE TEMP TABLE IF NOT EXISTS _wip2 (
    int1 int, 
    int2 int,
    seq int,
    geo_id numeric, 
    lf_name varchar, 
    ind_line_geom geometry,
    line_geom geometry, 
    new_line1 geometry,
    new_line2 geometry,
    section NUMRANGE,
    oid1_geom geometry, 
    oid1_geom_translated geometry, 
    oid2_geom geometry, 
    oid2_geom_translated geometry, 
    objectid numeric, 
    fcode int, 
    fcode_desc varchar, 
    lev_sum int, 
    line_geom_cut geometry,
    line_geom_reversed geometry,
    whole_centreline geometry,
    pgrout_centreline geometry
);

TRUNCATE TABLE _wip2; 

    _int1_result := gis._get_intersection_geom(highway2, btwn1, direction_btwn1, metres_btwn1, 0);

    _int2_result := (
        CASE WHEN btwn2_orig LIKE '%point%' AND (btwn2_check NOT LIKE '% of %' OR btwn2_check LIKE ('% of ' || TRIM(btwn1)))
            THEN gis._get_intersection_geom(highway2, btwn2, direction_btwn2, metres_btwn2, 0)
            ELSE gis._get_intersection_geom(highway2, btwn2, direction_btwn2, metres_btwn2, _int1_result.int_id_found)
        END
    );
                
    INSERT INTO _wip2(int1, int2, seq, geo_id, lf_name, ind_line_geom,
    oid1_geom, oid1_geom_translated, oid2_geom, oid2_geom_translated, objectid, fcode, fcode_desc, lev_sum)
    SELECT rout.int_start, rout.int_end, rout.seq, rout.geo_id, rout.lf_name, geom AS ind_line_geom, 
    _int1_result.oid_geom AS oid1_geom, _int1_result.oid_geom_translated AS oid1_geom_translated,
    _int2_result.oid_geom AS oid2_geom, _int2_result.oid_geom_translated AS oid2_geom_translated,
    rout.objectid, rout.fcode, rout.fcode_desc, _int1_result.lev_sum + _int2_result.lev_sum
    FROM gis._get_lines_btwn_interxn(highway2, _int1_result.int_id_found, _int2_result.int_id_found) rout;

--those centreline found from buffer are where int_start, int_end, seq, lev_sum = NULL
INSERT INTO _wip2 (geo_id, lf_name, ind_line_geom, new_line1, new_line2, 
oid1_geom, oid1_geom_translated, oid2_geom, oid2_geom_translated,
objectid, fcode, fcode_desc)

WITH get_int AS (
    SELECT
        _wip2.lf_name, _wip2.oid1_geom, _wip2.oid1_geom_translated, 
        _wip2.oid2_geom, _wip2.oid2_geom_translated, 
        ST_MakeLine(_wip2.oid1_geom, _wip2.oid1_geom_translated) AS new_line1,
        ST_MakeLine(_wip2.oid2_geom, _wip2.oid2_geom_translated) AS new_line2
    FROM _wip2 
    --The columns I want are all the same for each row anyway besides the centreline information ie geo_id
    LIMIT 1
)

SELECT
    cl.centreline_id AS geo_id,
    cl.linear_name_full AS lf_name,
    cl.geom, 
    get_int.new_line1,
    get_int.new_line2,
    get_int.oid1_geom,
    get_int.oid1_geom_translated, 
    get_int.oid2_geom,
    get_int.oid2_geom_translated, 
    cl.objectid,
    cl.feature_code AS fcode,
    cl.feature_code_desc AS fcode_desc
FROM gis_core.centreline_latest AS cl
JOIN get_int ON get_int.lf_name =  cl.linear_name_full --only get those with desired street names
WHERE
    cl.centreline_id NOT IN (SELECT _wip2.geo_id FROM _wip2)  --not repeating those found from pgrouting
    AND (
        ST_DWithin(
            ST_Transform(cl.geom, 2952), 
            ST_BUFFER(ST_Transform(get_int.new_line1, 2952), 3*metres_btwn1, 'endcap=flat join=round'),
            10) = TRUE 
        OR ST_DWithin(
            ST_Transform(cl.geom, 2952), 
            ST_BUFFER(ST_Transform(get_int.new_line2, 2952), 3*metres_btwn2, 'endcap=flat join=round'),
            10) = TRUE 
    )
--as some centreline is much longer compared to the short road segment, the ratio is set to 0.1 instead of 0.9
    AND (
        ST_Length(ST_Intersection(
        ST_Buffer(ST_Transform(get_int.new_line1, 2952), 3*(ST_Length(ST_Transform(get_int.new_line1, 2952))), 'endcap=flat join=round') , 
        ST_Transform(cl.geom, 2952))) / ST_Length(ST_Transform(cl.geom, 2952)) > 0.1 
        OR
        ST_Length(ST_Intersection(
        ST_Buffer(ST_Transform(get_int.new_line2, 2952), 3*(ST_Length(ST_Transform(get_int.new_line2, 2952))), 'endcap=flat join=round') , 
        ST_Transform(cl.geom, 2952))) / ST_Length(ST_Transform(cl.geom, 2952)) > 0.1
    );

RAISE NOTICE 'Centrelines within the buffer and have the same bylaws highway name are found.'; 

--TO CUT combined ind_line_geom and put into line_geom
UPDATE _wip2 SET whole_centreline =
(SELECT ST_LineMerge(ST_Union(_wip2.ind_line_geom)) 
FROM _wip2);

--TO COMBINE THOSE FOUND FROM PGROUTING (WHERE SEQ IS NOT NULL)
UPDATE _wip2 SET pgrout_centreline =
(SELECT ST_LineMerge(ST_Union(_wip2.ind_line_geom)) 
FROM _wip2
WHERE _wip2.seq IS NOT NULL);

--DEAL WITH oid1_geom_translated (FIRST INTERSECTION POINT)
UPDATE _wip2 SET line_geom_cut = (
CASE WHEN metres_btwn1 > ST_Length(ST_Transform(_wip2.pgrout_centreline, 2952)) 
AND metres_btwn1 - ST_Length(ST_Transform(_wip2.pgrout_centreline, 2952)) < 15
THEN _wip2.pgrout_centreline

--To check if the oid1_geom_translated point is within pgrout_centreline to determine if we should add or subtract
--routed centreline + additional centreline xx metres from that intersection
WHEN ST_Within(ST_Transform(ST_ClosestPoint(_wip2.whole_centreline, _wip2.oid1_geom_translated), 2952), ST_BUFFER(ST_Transform(_wip2.pgrout_centreline, 2952), 2, 'endcap=square join=round')) = FALSE
THEN (
    CASE WHEN ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid1_geom)
    > ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid1_geom_translated)
    THEN ST_Union(_wip2.pgrout_centreline ,
    ST_LineSubstring(_wip2.whole_centreline, 
    ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid1_geom) - (metres_btwn1/ST_Length(ST_Transform(_wip2.whole_centreline, 2952))),
    ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid1_geom) )  
        )
 
    WHEN ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid1_geom)
    < ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid1_geom_translated)
    THEN ST_Union(_wip2.pgrout_centreline ,
    ST_LineSubstring(_wip2.whole_centreline, 
    ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid1_geom),
    ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid1_geom) + (metres_btwn1/ST_Length(ST_Transform(_wip2.whole_centreline, 2952)))  )
        )
    
    END )

--routed centreline - part of centreline xx metres from that intersection that got trimmed
WHEN ST_Within(ST_Transform(ST_ClosestPoint(_wip2.whole_centreline, _wip2.oid1_geom_translated), 2952), ST_BUFFER(ST_Transform(_wip2.pgrout_centreline, 2952), 2, 'endcap=square join=round')) = TRUE
THEN (
    CASE WHEN ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid1_geom)
    < ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid1_geom_translated)
    THEN ST_Difference(_wip2.pgrout_centreline ,
    ST_LineSubstring(_wip2.pgrout_centreline, 
    ST_LineLocatePoint(_wip2.pgrout_centreline, _wip2.oid1_geom),
    ST_LineLocatePoint(_wip2.pgrout_centreline, _wip2.oid1_geom) + (metres_btwn1/ST_Length(ST_Transform(_wip2.pgrout_centreline, 2952)))  )
        )

    WHEN ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid1_geom)
    > ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid1_geom_translated)
    THEN ST_Difference(_wip2.pgrout_centreline ,
    ST_LineSubstring(_wip2.pgrout_centreline, 
    ST_LineLocatePoint(_wip2.pgrout_centreline, _wip2.oid1_geom) - (metres_btwn1/ST_Length(ST_Transform(_wip2.pgrout_centreline, 2952))),
    ST_LineLocatePoint(_wip2.pgrout_centreline, _wip2.oid1_geom) )  
        )

    END )

ELSE _wip2.pgrout_centreline

END);

RAISE NOTICE 'First intersection has been dealt with. Centrelines are now combined and cut (add/trim) as specified on the bylaws. 
direction_btwn2: %, metres_btwn2: %, whole_centreline: %, line_geom: %',
direction_btwn2, metres_btwn2, ST_ASText(ST_Union(_wip2.whole_centreline)) FROM _wip2, 
ST_ASText(ST_Union(_wip2.line_geom_cut)) FROM _wip2;

--DEAL WITH oid2_geom_translated (SECOND INTERSECTION POINT)
UPDATE _wip2 SET line_geom_cut = (
CASE WHEN metres_btwn2 > ST_Length(ST_Transform(_wip2.line_geom_cut, 2952)) 
AND metres_btwn2 - ST_Length(ST_Transform(_wip2.line_geom_cut, 2952)) < 15
THEN _wip2.line_geom_cut

--To check if the oid2_geom_translated point is within pgrout_centreline to determine if we should add or subtract
--routed centreline + additional centreline xx metres from that intersection
WHEN ST_Within(ST_Transform(ST_ClosestPoint(_wip2.whole_centreline, _wip2.oid2_geom_translated), 2952), ST_BUFFER(ST_Transform(_wip2.line_geom_cut, 2952), 2, 'endcap=square join=round')) = FALSE
THEN (
    CASE WHEN ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid2_geom)
    > ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid2_geom_translated)
    THEN ST_Union(_wip2.line_geom_cut ,
    ST_LineSubstring(_wip2.whole_centreline, 
    ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid2_geom) - (metres_btwn2/ST_Length(ST_Transform(_wip2.whole_centreline, 2952))),
    ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid2_geom) )  
    )
 
    WHEN ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid2_geom)
    < ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid2_geom_translated)
    THEN ST_Union(_wip2.line_geom_cut ,
    ST_LineSubstring(_wip2.whole_centreline, 
    ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid2_geom),
    ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid2_geom) + (metres_btwn2/ST_Length(ST_Transform(_wip2.whole_centreline, 2952)))  )
        )
    
    END )

--routed centreline - part of centreline xx metres from that intersection that got trimmed
WHEN ST_Within(ST_Transform(ST_ClosestPoint(_wip2.whole_centreline, _wip2.oid2_geom_translated), 2952), ST_BUFFER(ST_Transform(_wip2.line_geom_cut, 2952), 2, 'endcap=square join=round')) = TRUE
THEN (
    CASE WHEN ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid2_geom)
    < ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid2_geom_translated)
    THEN ST_Difference(_wip2.line_geom_cut ,
    ST_LineSubstring(_wip2.line_geom_cut, 
    ST_LineLocatePoint(_wip2.line_geom_cut, _wip2.oid2_geom),
    ST_LineLocatePoint(_wip2.line_geom_cut, _wip2.oid2_geom) + (metres_btwn2/ST_Length(ST_Transform(_wip2.line_geom_cut, 2952)))  )
        )

    WHEN ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid2_geom)
    > ST_LineLocatePoint(_wip2.whole_centreline, _wip2.oid2_geom_translated)
    THEN ST_Difference(_wip2.line_geom_cut ,
    ST_LineSubstring(_wip2.line_geom_cut, 
    ST_LineLocatePoint(_wip2.line_geom_cut, _wip2.oid2_geom) - (metres_btwn2/ST_Length(ST_Transform(_wip2.line_geom_cut, 2952))),
    ST_LineLocatePoint(_wip2.line_geom_cut, _wip2.oid2_geom) )  
        )

    END )

ELSE _wip2.line_geom_cut

END);

RAISE NOTICE 'Second intersection has been dealt with. Centrelines are now combined and cut (add/trim) as specified on the bylaws. 
direction_btwn2: %, metres_btwn2: %, whole_centreline: %, line_geom: %',
direction_btwn2, metres_btwn2, ST_ASText(ST_Union(_wip2.whole_centreline)) FROM _wip2, 
ST_ASText(ST_Union(_wip2.line_geom_cut)) FROM _wip2;

--TO SEPARATE line_geom that got cut into individual rows with section stated
UPDATE _wip2 SET line_geom_reversed = (
CASE WHEN ABS(DEGREES(ST_Azimuth(ST_StartPoint(_wip2.ind_line_geom), ST_EndPoint(_wip2.ind_line_geom))) 
       - DEGREES(ST_Azimuth(ST_StartPoint(_wip2.line_geom_cut), ST_EndPoint(_wip2.line_geom_cut)))
       ) > 180 
--The lines are in two different orientations
THEN ST_LineMerge(ST_Reverse(_wip2.line_geom_cut))
ELSE ST_LineMerge(_wip2.line_geom_cut)
END
);

--ST_LineMerge to make sure that the section part is working right
UPDATE _wip2 SET ind_line_geom = ST_LineMerge(_wip2.ind_line_geom);

UPDATE _wip2 SET section = (
--where the whole centreline is within the buffer
CASE WHEN ST_Within(ST_Transform(_wip2.ind_line_geom, 2952), ST_BUFFER(ST_Transform(_wip2.line_geom_reversed, 2952), 2, 'endcap=square join=round')) = TRUE
THEN numrange(0, 1, '[]')

--where part of the centreline is within the buffer, and then find out the startpoint of the individual centreline to know which part of the centreline needs to be cut
WHEN ST_Within(ST_StartPoint(ST_Transform(_wip2.ind_line_geom, 2952)), ST_BUFFER(ST_Transform(_wip2.line_geom_reversed, 2952), 2, 'endcap=square join=round')) = TRUE
THEN numrange(0, (ST_LineLocatePoint(_wip2.ind_line_geom, ST_EndPoint(_wip2.line_geom_reversed)))::numeric, '[]')

WHEN ST_Within(ST_EndPoint(ST_Transform(_wip2.ind_line_geom, 2952)), ST_BUFFER(ST_Transform(_wip2.line_geom_reversed, 2952), 2, 'endcap=square join=round')) = TRUE
THEN numrange((ST_LineLocatePoint(_wip2.ind_line_geom, ST_StartPoint(_wip2.line_geom_reversed)))::numeric, 1, '[]')    

ELSE NULL

END);

UPDATE _wip2 SET line_geom = (
--where the whole centreline is within the buffer
CASE WHEN ST_Within(ST_Transform(_wip2.ind_line_geom, 2952), ST_BUFFER(ST_Transform(_wip2.line_geom_reversed, 2952), 2, 'endcap=square join=round')) = TRUE
THEN _wip2.ind_line_geom

--where part of the centreline is within the buffer
ELSE ST_Intersection(ST_Buffer(_wip2.line_geom_reversed, 0.00001), _wip2.ind_line_geom)

END);

RAISE NOTICE 'Centrelines are now separated into their respective geo_id rows.';


RETURN QUERY
SELECT int1, int2, _wip2.seq, _wip2.geo_id, _wip2.lf_name, 
_wip2.ind_line_geom, _wip2.line_geom, _wip2.line_geom_cut, _wip2.section,
_wip2.oid1_geom, _wip2.oid1_geom_translated, _wip2.oid2_geom, _wip2.oid2_geom_translated, 
_wip2.objectid, _wip2.fcode, _wip2.fcode_desc, _wip2.lev_sum
FROM _wip2;

DROP TABLE _wip2;

EXCEPTION WHEN SQLSTATE 'XX000' THEN
    RAISE WARNING 'Internal error at case2 for highway2 = % , btwn1 = %, btwn2 = % : ''%'' ', 
    highway2, btwn1, btwn2, SQLERRM ;

END;
$BODY$;
