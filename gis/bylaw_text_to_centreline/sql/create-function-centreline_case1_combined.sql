DROP FUNCTION jchew._centreline_case1_combined(text, text, text, double precision);
CREATE OR REPLACE FUNCTION jchew._centreline_case1_combined(highway2 TEXT, btwn2 TEXT, direction_btwn2 TEXT, metres_btwn2 FLOAT)
RETURNS TABLE(int1 INTEGER, geo_id NUMERIC, lf_name VARCHAR, 
ind_line_geom GEOMETRY, line_geom GEOMETRY, line_geom_cut GEOMETRY, section NUMRANGE,
oid1_geom GEOMETRY, oid1_geom_translated GEOMETRY, objectid NUMERIC, fcode INTEGER, fcode_desc VARCHAR, 
lev_sum INTEGER, dist_from_pt FLOAT, dist_from_translated_pt FLOAT)

LANGUAGE 'plpgsql'
AS 
$BODY$

DECLARE

whole_centreline GEOMETRY;
line_geom_reversed GEOMETRY;

BEGIN

CREATE TEMP TABLE IF NOT EXISTS _wip(
    int1 INTEGER, 
    geo_id NUMERIC, 
    lf_name VARCHAR, 
    ind_line_geom GEOMETRY,
    line_geom GEOMETRY, 
    new_line GEOMETRY,
    section NUMRANGE,
    oid1_geom GEOMETRY, 
    oid1_geom_translated GEOMETRY, 
    objectid NUMERIC, 
    fcode INTEGER, 
    fcode_desc VARCHAR, 
    lev_sum INTEGER, 
    dist_from_pt FLOAT, 
    dist_from_translated_pt FLOAT,
    line_geom_cut GEOMETRY,
    combined_section NUMRANGE
);

INSERT INTO _wip (int1, geo_id, lf_name, ind_line_geom, new_line, oid1_geom, oid1_geom_translated, objectid, fcode, 
    fcode_desc, lev_sum, dist_from_pt, dist_from_translated_pt)
WITH get_int AS
(SELECT oid_geom AS oid1_geom, oid_geom_translated AS oid1_geom_translated, 
ST_MakeLine(oid_geom, oid_geom_translated) AS new_line, -- line from the intersection point to the translated point
int_id_found AS int1, get_geom.lev_sum
FROM jchew._get_intersection_geom_updated(highway2, btwn2, direction_btwn2, metres_btwn2, 0) get_geom)

, get_line AS (
SELECT *, 
ST_Distance(ST_Transform(a.oid1_geom,2952), ST_Transform(a.geom,2952)) AS dist_from_pt,
ST_Distance(ST_Transform(a.oid1_geom_translated,2952), ST_Transform(a.geom,2952)) AS dist_from_translated_pt
FROM 

(SELECT cl.geo_id, cl.lf_name, cl.objectid, cl.fcode, cl.fcode_desc, cl.geom, get_int.oid1_geom, get_int.oid1_geom_translated,
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
	ST_Transform(cl.geom, 2952))) / ST_Length(ST_Transform(cl.geom, 2952)) > 0.1 ) a 
	
WHERE a.lf_name = highway2 

)

SELECT get_int.int1, get_line.geo_id, get_line.lf_name, get_line.geom AS ind_line_geom, get_int.new_line, 
get_int.oid1_geom, get_int.oid1_geom_translated, get_line.objectid, get_line.fcode, get_line.fcode_desc, 
get_int.lev_sum, get_line.dist_from_pt, get_line.dist_from_translated_pt
FROM get_int, get_line;

RAISE NOTICE 'Centrelines within the buffer and have the same bylaws highway name is found.'; 

--TO CUT combined ind_line_geom and put into line_geom
whole_centreline := ST_LineMerge(ST_Union(_wip.ind_line_geom)) FROM _wip;

UPDATE _wip SET line_geom_cut = (
CASE WHEN metres_btwn2 > ST_Length(ST_Transform(whole_centreline, 2952)) 
AND metres_btwn2 - ST_Length(ST_Transform(whole_centreline, 2952)) < 15
THEN whole_centreline

-- metres_btwn2/ST_Length(d.geom) is the fraction that is supposed to be cut off from the dissolved centreline segment(s)
-- cut off the first fraction of the dissolved line, and the second and check to see which one is closer to the original interseciton
-- aka to get the starting point of how the line is drawn and then cut accordingly

--when the from_intersection is at the end point of the original centreline
WHEN ST_LineLocatePoint(whole_centreline, _wip.oid1_geom)
> ST_LineLocatePoint(whole_centreline, ST_ClosestPoint(whole_centreline, ST_EndPoint(_wip.new_line)))
THEN ST_LineSubstring(whole_centreline, 
ST_LineLocatePoint(whole_centreline, _wip.oid1_geom) - (metres_btwn2/ST_Length(ST_Transform(whole_centreline, 2952))),
ST_LineLocatePoint(whole_centreline, _wip.oid1_geom) )

--when the from_intersection is at the start point of the original centreline 
WHEN ST_LineLocatePoint(whole_centreline, _wip.oid1_geom)
< ST_LineLocatePoint(whole_centreline, ST_ClosestPoint(whole_centreline, ST_EndPoint(_wip.new_line)))
-- take the substring from the intersection to the point x metres ahead of it
THEN ST_LineSubstring(whole_centreline, 
ST_LineLocatePoint(whole_centreline, _wip.oid1_geom),
ST_LineLocatePoint(whole_centreline, _wip.oid1_geom) + (metres_btwn2/ST_Length(ST_Transform(whole_centreline, 2952)))  )

END
);

UPDATE _wip SET combined_section = (
-- case where the section of street from the intersection in the specified direction is shorter than x metres
--take the whole centreline, range is [0,1]
CASE WHEN metres_btwn2 > ST_Length(ST_Transform(whole_centreline, 2952)) 
AND metres_btwn2 - ST_Length(ST_Transform(whole_centreline, 2952)) < 15
THEN numrange(0, 1, '[]')

--when the from_intersection is at the end point of the original centreline
--range is [xxx, 1]
WHEN ST_LineLocatePoint(whole_centreline, _wip.oid1_geom)
> ST_LineLocatePoint(whole_centreline, ST_ClosestPoint(whole_centreline, ST_EndPoint(new_line)))
THEN numrange ((ST_LineLocatePoint(whole_centreline, _wip.oid1_geom) - (metres_btwn2/ST_Length(ST_Transform(whole_centreline, 2952))))::numeric , 1::numeric, '[]')

--when the from_intersection is at the start point of the original centreline 
--range is [0, xxx]
WHEN ST_LineLocatePoint(whole_centreline, _wip.oid1_geom)
< ST_LineLocatePoint(whole_centreline, ST_ClosestPoint(whole_centreline, ST_EndPoint(new_line)))
-- take the substring from the intersection to the point x metres ahead of it
THEN numrange(0::numeric, (ST_LineLocatePoint(whole_centreline, _wip.oid1_geom) + (metres_btwn2/ST_Length(ST_Transform(whole_centreline, 2952))))::numeric, '[]')

END
);

RAISE NOTICE 'Centrelines are now combined and cut as specified on the bylaws. 
direction_btwn2: %, metres_btwn2: %  whole_centreline: %  line_geom: %',
direction_btwn2, metres_btwn2, ST_ASText(whole_centreline), 
ST_ASText(ST_Union(_wip.line_geom_cut)) FROM _wip;

--TO SEPARATE line_geom that got cut into individual rows with section stated
IF ABS(DEGREES(ST_azimuth(ST_StartPoint(ind_line_geom), ST_EndPoint(ind_line_geom))) 
       - DEGREES(ST_azimuth(ST_StartPoint(line_geom_cut), ST_EndPoint(line_geom_cut)))
       ) > 180 THEN
--The lines are two different orientations
line_geom_reversed := ST_Reverse(line_geom_cut);
END IF;

UPDATE _wip SET section = (
--combined_section = '[0,1]'
CASE WHEN lower(_wip.combined_section) = 0 AND upper(_wip.combined_section) = 1
THEN numrange(0, 1,'[]')

--for combined_section = '[%,1]' or '[0,%]'
--where the whole centreline is within the buffer
WHEN ST_Within(ST_Transform(_wip.ind_line_geom, 2952), ST_BUFFER(ST_Transform(line_geom_reversed, 2952), 2, 'endcap=square join=round')) = TRUE
THEN numrange(0, 1, '[]')

--where part of the centreline is within the buffer, and then find out the startpoint of the individual centreline to know which part of the centreline needs to be cut
WHEN ST_Within(ST_StartPoint(ST_Transform(_wip.ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_reversed, 2952), 2, 'endcap=square join=round')) = TRUE
THEN numrange(0, (ST_LineLocatePoint(_wip.ind_line_geom, ST_EndPoint(line_geom_reversed)))::numeric, '[]')

WHEN ST_Within(ST_EndPoint(ST_Transform(_wip.ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_reversed, 2952), 2, 'endcap=square join=round')) = TRUE
THEN numrange((ST_LineLocatePoint(_wip.ind_line_geom, ST_StartPoint(line_geom_reversed)))::numeric, 1, '[]')    

ELSE NULL

END);

UPDATE _wip SET line_geom = (
--combined_section = '[0,1]'
CASE WHEN lower(_wip.combined_section) = 0 AND upper(_wip.combined_section) = 1
THEN _wip.ind_line_geom

--for combined_section = '[%,1]' or '[0,%]'
--where the whole centreline is within the buffer
WHEN ST_Within(ST_Transform(_wip.ind_line_geom, 2952), ST_BUFFER(ST_Transform(line_geom_reversed, 2952), 2, 'endcap=square join=round')) = TRUE
THEN _wip.ind_line_geom

--where part of the centreline is within the buffer
ELSE ST_Intersection(ST_Buffer(line_geom_reversed, 0.00001), _wip.ind_line_geom)

END);

RAISE NOTICE 'Centrelines are now separated into their respective geo_id row.';

RETURN QUERY
SELECT _wip.int1, _wip.geo_id, _wip.lf_name, _wip.ind_line_geom, _wip.line_geom, _wip.line_geom_cut, _wip.section, 
_wip.oid1_geom, _wip.oid1_geom_translated, _wip.objectid, _wip.fcode, _wip.fcode_desc, 
_wip.lev_sum, _wip.dist_from_pt, _wip.dist_from_translated_pt FROM _wip;

DROP TABLE _wip;

END;
$BODY$;
