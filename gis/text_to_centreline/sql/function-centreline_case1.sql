DROP FUNCTION gis._centreline_case1 (text, text, text, double precision);

CREATE OR REPLACE FUNCTION gis._centreline_case1(
    highway2 text,
    btwn2 text,
    direction_btwn2 text,
    metres_btwn2 double precision
)
RETURNS TABLE (
    int1 integer,
    geo_id numeric,
    lf_name character varying,
    ind_line_geom geometry,
    line_geom geometry,
    line_geom_cut geometry,
    section numrange,
    combined_section numrange,
    oid1_geom geometry,
    oid1_geom_translated geometry,
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

BEGIN

CREATE TEMP TABLE IF NOT EXISTS _wip(
    int1 int, 
    geo_id NUMERIC, 
    lf_name VARCHAR, 
    ind_line_geom GEOMETRY,
    line_geom GEOMETRY, 
    new_line GEOMETRY,
    section NUMRANGE,
    oid1_geom GEOMETRY, 
    oid1_geom_translated GEOMETRY, 
    objectid NUMERIC, 
    fcode int, 
    fcode_desc VARCHAR, 
    lev_sum int, 
    line_geom_cut GEOMETRY,
    line_geom_reversed GEOMETRY,
    combined_section NUMRANGE,
    whole_centreline GEOMETRY
);

TRUNCATE TABLE _wip;

INSERT INTO _wip (int1, geo_id, lf_name, ind_line_geom, new_line, oid1_geom, oid1_geom_translated, 
objectid, fcode, fcode_desc, lev_sum)
WITH get_int AS
(SELECT oid_geom AS oid1_geom, oid_geom_translated AS oid1_geom_translated, 
ST_MakeLine(oid_geom, oid_geom_translated) AS new_line, -- line from the intersection point to the translated point
int_id_found AS int1, get_geom.lev_sum
FROM gis._get_intersection_geom(highway2, btwn2, direction_btwn2, metres_btwn2, 0) get_geom)
,
get_lines AS (
SELECT cl.geo_id, cl.lf_name, cl.objectid, cl.fcode, cl.fcode_desc, cl.geom, get_int.oid1_geom, get_int.oid1_geom_translated,
ST_DWithin(ST_Transform(cl.geom, 2952), 
           ST_BUFFER(ST_Transform(get_int.new_line, 2952), 3*metres_btwn2, 'endcap=flat join=round'),
           10) AS dwithin
FROM gis.centreline cl, get_int
WHERE ST_DWithin(ST_Transform(cl.geom, 2952), 
           ST_BUFFER(ST_Transform(get_int.new_line, 2952), 3*metres_btwn2, 'endcap=flat join=round'),
           10) = TRUE 
--as some centreline is much longer compared to the short road segment, the ratio is set to 0.1 instead of 0.9
AND ST_Length(ST_Intersection(
    ST_Buffer(ST_Transform(get_int.new_line, 2952), 3*(ST_Length(ST_Transform(get_int.new_line, 2952))), 'endcap=flat join=round') , 
    ST_Transform(cl.geom, 2952))) / ST_Length(ST_Transform(cl.geom, 2952)) > 0.1 
)

SELECT get_int.int1, get_lines.geo_id, get_lines.lf_name, ST_LineMerge(get_lines.geom) AS ind_line_geom, get_int.new_line, 
get_int.oid1_geom, get_int.oid1_geom_translated, get_lines.objectid, get_lines.fcode, get_lines.fcode_desc, 
get_int.lev_sum
FROM get_int, get_lines
WHERE get_lines.lf_name = highway2 ;

RAISE NOTICE 'Centrelines within the buffer and have the same bylaws highway name are found.'; 

--TO CUT combined ind_line_geom and put into line_geom
UPDATE _wip SET whole_centreline =
(SELECT ST_LineMerge(ST_Union(_wip.ind_line_geom)) 
FROM _wip
GROUP BY _wip.lf_name );

UPDATE _wip SET line_geom_cut = (
CASE WHEN metres_btwn2 > ST_Length(ST_Transform(_wip.whole_centreline, 2952)) 
AND metres_btwn2 - ST_Length(ST_Transform(_wip.whole_centreline, 2952)) < 15
THEN _wip.whole_centreline

-- metres_btwn2/ST_Length(d.geom) is the fraction that is supposed to be cut off from the dissolved centreline segment(s)
-- cut off the first fraction of the dissolved line, and the second and check to see which one is closer to the original interseciton
-- aka to get the starting point of how the line is drawn and then cut accordingly

--when the from_intersection is at the end point of the original centreline
WHEN ST_LineLocatePoint(_wip.whole_centreline, _wip.oid1_geom)
> ST_LineLocatePoint(_wip.whole_centreline, ST_ClosestPoint(_wip.whole_centreline, ST_EndPoint(_wip.new_line)))
THEN ST_LineSubstring(_wip.whole_centreline, 
ST_LineLocatePoint(_wip.whole_centreline, _wip.oid1_geom) - (metres_btwn2/ST_Length(ST_Transform(_wip.whole_centreline, 2952))),
ST_LineLocatePoint(_wip.whole_centreline, _wip.oid1_geom) )

--when the from_intersection is at the start point of the original centreline 
WHEN ST_LineLocatePoint(_wip.whole_centreline, _wip.oid1_geom)
< ST_LineLocatePoint(_wip.whole_centreline, ST_ClosestPoint(_wip.whole_centreline, ST_EndPoint(_wip.new_line)))
-- take the substring from the intersection to the point x metres ahead of it
THEN ST_LineSubstring(_wip.whole_centreline, 
ST_LineLocatePoint(_wip.whole_centreline, _wip.oid1_geom),
ST_LineLocatePoint(_wip.whole_centreline, _wip.oid1_geom) + (metres_btwn2/ST_Length(ST_Transform(_wip.whole_centreline, 2952)))  )

END
);

UPDATE _wip SET combined_section = (
-- case where the section of street from the intersection in the specified direction is shorter than x metres
--take the whole centreline, range is [0,1]
CASE WHEN metres_btwn2 > ST_Length(ST_Transform(_wip.whole_centreline, 2952)) 
AND metres_btwn2 - ST_Length(ST_Transform(_wip.whole_centreline, 2952)) < 15
THEN numrange(0, 1, '[]')

--when the from_intersection is at the end point of the original centreline
--range is [xxx, 1]
WHEN ST_LineLocatePoint(_wip.whole_centreline, _wip.oid1_geom)
> ST_LineLocatePoint(_wip.whole_centreline, ST_ClosestPoint(_wip.whole_centreline, ST_EndPoint(new_line)))
THEN numrange ((ST_LineLocatePoint(_wip.whole_centreline, _wip.oid1_geom) - (metres_btwn2/ST_Length(ST_Transform(_wip.whole_centreline, 2952))))::numeric , 1::numeric, '[]')

--when the from_intersection is at the start point of the original centreline 
--range is [0, xxx]
WHEN ST_LineLocatePoint(_wip.whole_centreline, _wip.oid1_geom)
< ST_LineLocatePoint(_wip.whole_centreline, ST_ClosestPoint(_wip.whole_centreline, ST_EndPoint(new_line)))
-- take the substring from the intersection to the point x metres ahead of it
THEN numrange(0::numeric, (ST_LineLocatePoint(_wip.whole_centreline, _wip.oid1_geom) + (metres_btwn2/ST_Length(ST_Transform(_wip.whole_centreline, 2952))))::numeric, '[]')

END
);

RAISE NOTICE 'Centrelines are now combined and cut as specified on the bylaws. 
direction_btwn2: %, metres_btwn2: %, whole_centreline: %, line_geom: %',
direction_btwn2, metres_btwn2, ST_ASText(ST_Union(_wip.whole_centreline)) FROM _wip, 
ST_ASText(ST_Union(_wip.line_geom_cut)) FROM _wip;

--TO SEPARATE line_geom that got cut into individual rows with section stated
UPDATE _wip SET line_geom_reversed = (
--TO SEPARATE line_geom that got cut into individual rows with section stated
CASE WHEN ABS(DEGREES(ST_Azimuth(ST_StartPoint(_wip.ind_line_geom), ST_EndPoint(_wip.ind_line_geom))) 
       - DEGREES(ST_Azimuth(ST_StartPoint(_wip.line_geom_cut), ST_EndPoint(_wip.line_geom_cut)))
       ) > 180 
--The lines are two different orientations
THEN ST_Reverse(_wip.line_geom_cut)
ELSE _wip.line_geom_cut
END
);

UPDATE _wip SET section = (
--combined_section = '[0,1]'
CASE WHEN lower(_wip.combined_section) = 0 AND upper(_wip.combined_section) = 1
THEN numrange(0, 1,'[]')

--for combined_section = '[%,1]' or '[0,%]'
--where the whole centreline is within the buffer
WHEN ST_Within(ST_Transform(_wip.ind_line_geom, 2952), ST_BUFFER(ST_Transform(_wip.line_geom_reversed, 2952), 2, 'endcap=square join=round')) = TRUE
THEN numrange(0, 1, '[]')

--where part of the centreline is within the buffer, and then find out the startpoint of the individual centreline to know which part of the centreline needs to be cut
WHEN ST_Within(ST_StartPoint(ST_Transform(_wip.ind_line_geom, 2952)), ST_BUFFER(ST_Transform(_wip.line_geom_reversed, 2952), 2, 'endcap=square join=round')) = TRUE
THEN numrange(0, (ST_LineLocatePoint(_wip.ind_line_geom, ST_EndPoint(_wip.line_geom_reversed)))::numeric, '[]')

WHEN ST_Within(ST_EndPoint(ST_Transform(_wip.ind_line_geom, 2952)), ST_BUFFER(ST_Transform(_wip.line_geom_reversed, 2952), 2, 'endcap=square join=round')) = TRUE
THEN numrange((ST_LineLocatePoint(_wip.ind_line_geom, ST_StartPoint(_wip.line_geom_reversed)))::numeric, 1, '[]')    

ELSE NULL

END);

UPDATE _wip SET line_geom = (
--combined_section = '[0,1]'
CASE WHEN lower(_wip.combined_section) = 0 AND upper(_wip.combined_section) = 1
THEN _wip.ind_line_geom

--for combined_section = '[%,1]' or '[0,%]'
--where the whole centreline is within the buffer
WHEN ST_Within(ST_Transform(_wip.ind_line_geom, 2952), ST_BUFFER(ST_Transform(_wip.line_geom_reversed, 2952), 2, 'endcap=square join=round')) = TRUE
THEN _wip.ind_line_geom

--where part of the centreline is within the buffer
ELSE ST_Intersection(ST_Buffer(_wip.line_geom_reversed, 0.00001), _wip.ind_line_geom)

END);

RAISE NOTICE 'Centrelines are now separated into their respective geo_id rows.';

RETURN QUERY
SELECT _wip.int1, _wip.geo_id, _wip.lf_name, 
_wip.ind_line_geom, _wip.line_geom, _wip.line_geom_cut, 
_wip.section, _wip.combined_section,
_wip.oid1_geom, _wip.oid1_geom_translated, 
_wip.objectid, _wip.fcode, _wip.fcode_desc, _wip.lev_sum 
FROM _wip;

DROP TABLE _wip;

EXCEPTION WHEN SQLSTATE 'XX000' THEN
    RAISE WARNING 'Internal error at case2 for highway2 = % , btwn2 = % : ''%'' ', 
    highway2, btwn2, SQLERRM ;

END;
$BODY$;

ALTER FUNCTION gis._centreline_case1(text, text, text, double precision)
OWNER TO gis_admins;
