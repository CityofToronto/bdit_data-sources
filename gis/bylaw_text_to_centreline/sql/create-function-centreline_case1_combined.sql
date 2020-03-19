--DROP FUNCTION jchew._centreline_case1_combined(text, text, text, double precision);
CREATE OR REPLACE FUNCTION jchew._centreline_case1_combined(highway2 TEXT, btwn2 TEXT, direction_btwn2 TEXT, metres_btwn2 FLOAT)
RETURNS TABLE(int1 INTEGER, geo_id NUMERIC, lf_name VARCHAR, line_geom GEOMETRY, new_line GEOMETRY, section NUMRANGE,
oid1_geom GEOMETRY, oid1_geom_translated GEOMETRY, objectid NUMERIC, fcode INTEGER, fcode_desc VARCHAR, 
lev_sum INTEGER, dist_from_pt FLOAT, dist_from_translated_pt FLOAT)

LANGUAGE 'plpgsql'
AS 
$BODY$

DECLARE

whole_centreline GEOMETRY;
line_geom_cut GEOMETRY;
combined_section NUMRANGE;
section NUMRANGE;
line_geom_separated GEOMETRY;

BEGIN

CREATE TEMP TABLE IF NOT EXISTS _wip(
    int1 INTEGER, 
    geo_id NUMERIC, 
    lf_name VARCHAR, 
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
    dist_from_translated_pt FLOAT
);

INSERT INTO _wip (int1, geo_id, lf_name, line_geom, new_line,oid1_geom, oid1_geom_translated, objectid, fcode, 
    fcode_desc, lev_sum, dist_from_pt, dist_from_translated_pt)
WITH X AS
(SELECT oid_geom AS oid1_geom, oid_geom_translated AS oid1_geom_translated, 
ST_MakeLine(oid_geom, oid_geom_translated) AS new_line, -- line from the intersection point to the translated point
int_id_found AS int1, get_geom.lev_sum
FROM jchew._get_intersection_geom_updated(highway2, btwn2, direction_btwn2, metres_btwn2, 0) get_geom)

, Y AS (
SELECT *, 
ST_Distance(ST_Transform(a.oid1_geom,2952), ST_Transform(a.geom,2952)) AS dist_from_pt,
ST_Distance(ST_Transform(a.oid1_geom_translated,2952), ST_Transform(a.geom,2952)) AS dist_from_translated_pt
FROM 

(SELECT cl.geo_id, cl.lf_name, cl.objectid, cl.fcode, cl.fcode_desc, cl.geom, X.oid1_geom, X.oid1_geom_translated,
ST_DWithin(ST_Transform(cl.geom, 2952), 
		   ST_BUFFER(ST_Transform(X.new_line, 2952), 3*metres_btwn2, 'endcap=flat join=round'),
		   10) AS dwithin
FROM gis.centreline cl, X
WHERE ST_DWithin(ST_Transform(cl.geom, 2952), 
		   ST_BUFFER(ST_Transform(X.new_line, 2952), 3*metres_btwn2, 'endcap=flat join=round'),
		   10) = TRUE 
--as some centreline is much longer compared to the short road segment, the ratio is set to 0.1 instead of 0.9
AND ST_Length(ST_Intersection(
	ST_Buffer(ST_Transform(X.new_line, 2952), 3*(ST_Length(ST_Transform(X.new_line, 2952))), 'endcap=flat join=round') , 
	ST_Transform(cl.geom, 2952))) / ST_Length(ST_Transform(cl.geom, 2952)) > 0.1 ) a 
	
WHERE a.lf_name = highway2 

)

SELECT X.int1, Y.geo_id, Y.lf_name, Y.geom AS line_geom, X.new_line, X.oid1_geom, X.oid1_geom_translated, Y.objectid, Y.fcode, Y.fcode_desc, 
X.lev_sum, Y.dist_from_pt, Y.dist_from_translated_pt
FROM X, Y;

RAISE NOTICE 'Centrelines within the buffer and have the same bylaws highway name is found.'; 

whole_centreline := (SELECT ST_LineMerge(ST_Union(_wip.line_geom)) FROM _wip);

line_geom_cut := (
-- case where the section of street from the intersection in the specified direction is shorter than x metres
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

combined_section := (
-- case where the section of street from the intersection in the specified direction is shorter than x metres
--take the whole centreline, range is [0,1]
CASE WHEN metres_btwn2 > ST_Length(ST_Transform(whole_centreline, 2952)) 
AND metres_btwn2 - ST_Length(ST_Transform(whole_centreline, 2952)) < 15
THEN numrange(0, 1, '[]')

--when the from_intersection is at the end point of the original centreline
--range is [xxx, 1]
WHEN ST_LineLocatePoint(whole_centreline, _wip.oid1_geom)
> ST_LineLocatePoint(whole_centreline, ST_ClosestPoint(whole_centreline, ST_EndPoint(_wip.new_line)))
THEN numrange ((ST_LineLocatePoint(whole_centreline, _wip.oid1_geom) - (metres_btwn2/ST_Length(ST_Transform(whole_centreline, 2952))))::numeric , 1::numeric, '[]')

--when the from_intersection is at the start point of the original centreline 
--range is [0, xxx]
WHEN ST_LineLocatePoint(whole_centreline, _wip.oid1_geom)
< ST_LineLocatePoint(whole_centreline, ST_ClosestPoint(whole_centreline, ST_EndPoint(_wip.new_line)))
-- take the substring from the intersection to the point x metres ahead of it
THEN numrange(0::numeric, (ST_LineLocatePoint(whole_centreline, _wip.oid1_geom) + (metres_btwn2/ST_Length(ST_Transform(whole_centreline, 2952))))::numeric, '[]')

END
);

RAISE NOTICE 'Centrelines are now combined and cut as specified on the bylaws. direction_btwn2: %, metres_btwn2: %  whole_centreline: %  new_line: %  oid1_geom: % line_geom_cut: %',
direction_btwn2, metres_btwn2, ST_ASText(whole_centreline), ST_ASText(new_line), ST_ASText(oid1_geom), ST_ASText(line_geom_cut);


section :=
--combined_section = '[0,1]'
(CASE WHEN lower(combined_section) = 0 AND upper(combined_section) = 1
THEN numrange(0, 1,'[]')

--for combined_section = '[%,1]' or '[0,%]'
--where the whole centreline is within the buffer
WHEN ST_Within(ST_Transform(ind_line_geom, 2952), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=square join=round')) = TRUE
THEN numrange(0, 1, '[]')

--where part of the centreline is within the buffer, and then find out the startpoint of the individual centreline to know which part of the centreline needs to be cut
WHEN ST_Within(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=square join=round')) = TRUE
AND ST_Distance(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_EndPoint(ST_Transform(line_geom_cut, 2952))) < 
    ST_Distance(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_StartPoint(ST_Transform(line_geom_cut, 2952)))
THEN numrange(0, (ST_LineLocatePoint(ind_line_geom, ST_StartPoint(line_geom_cut)))::numeric, '[]')

WHEN ST_Within(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=square join=round')) = TRUE
AND ST_Distance(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_EndPoint(ST_Transform(line_geom_cut, 2952))) > 
    ST_Distance(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_StartPoint(ST_Transform(line_geom_cut, 2952)))
THEN numrange(0, (ST_LineLocatePoint(ind_line_geom, ST_EndPoint(line_geom_cut)))::numeric, '[]')

WHEN ST_Within(ST_EndPoint(ST_Transform(ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=square join=round')) = TRUE
AND ST_Distance(ST_EndPoint(ST_Transform(ind_line_geom, 2952)), ST_EndPoint(ST_Transform(line_geom_cut, 2952))) > 
    ST_Distance(ST_EndPoint(ST_Transform(ind_line_geom, 2952)), ST_StartPoint(ST_Transform(line_geom_cut, 2952)))
THEN numrange((ST_LineLocatePoint(ind_line_geom, ST_EndPoint(line_geom_cut)))::numeric, 1, '[]')   

WHEN ST_Within(ST_EndPoint(ST_Transform(ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=square join=round')) = TRUE
AND ST_Distance(ST_EndPoint(ST_Transform(ind_line_geom, 2952)), ST_EndPoint(ST_Transform(line_geom_cut, 2952))) < 
    ST_Distance(ST_EndPoint(ST_Transform(ind_line_geom, 2952)), ST_StartPoint(ST_Transform(line_geom_cut, 2952)))
THEN numrange((ST_LineLocatePoint(ind_line_geom, ST_StartPoint(line_geom_cut)))::numeric, 1, '[]')    

ELSE NULL

END);


line_geom_separated :=
--combined_section = '[0,1]'
(CASE WHEN lower(combined_section) = 0 AND upper(combined_section) = 1
THEN ind_line_geom

--for combined_section = '[%,1]' or '[0,%]'
--where the whole centreline is within the buffer
WHEN ST_Within(ST_Transform(ind_line_geom, 2952), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=square join=round')) = TRUE
THEN ind_line_geom

--where part of the centreline is within the buffer, and then find out the startpoint of the individual centreline
--and startpoint of the line_geom_cut to know which part of the centreline needs to be cut
WHEN ST_Within(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=square join=round')) = TRUE
AND ST_Distance(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_EndPoint(ST_Transform(line_geom_cut, 2952))) < 
    ST_Distance(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_StartPoint(ST_Transform(line_geom_cut, 2952)))
THEN ST_LineSubstring(ind_line_geom, 0 , ST_LineLocatePoint(ind_line_geom, ST_StartPoint(line_geom_cut)))

WHEN ST_Within(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=square join=round')) = TRUE
AND ST_Distance(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_EndPoint(ST_Transform(line_geom_cut, 2952))) > 
    ST_Distance(ST_StartPoint(ST_Transform(ind_line_geom, 2952)), ST_StartPoint(ST_Transform(line_geom_cut, 2952)))
THEN ST_LineSubstring(ind_line_geom, 0 , ST_LineLocatePoint(ind_line_geom, ST_EndPoint(line_geom_cut)))

WHEN ST_Within(ST_EndPoint(ST_Transform(ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=square join=round')) = TRUE
AND ST_Distance(ST_EndPoint(ST_Transform(ind_line_geom, 2952)), ST_EndPoint(ST_Transform(line_geom_cut, 2952))) > 
    ST_Distance(ST_EndPoint(ST_Transform(ind_line_geom, 2952)), ST_StartPoint(ST_Transform(line_geom_cut, 2952)))
THEN ST_LineSubstring(ind_line_geom, ST_LineLocatePoint(ind_line_geom, ST_EndPoint(line_geom_cut)), 1)  

WHEN ST_Within(ST_EndPoint(ST_Transform(ind_line_geom, 2952)), ST_BUFFER(ST_Transform(line_geom_cut, 2952), 2, 'endcap=square join=round')) = TRUE
AND ST_Distance(ST_EndPoint(ST_Transform(ind_line_geom, 2952)), ST_EndPoint(ST_Transform(line_geom_cut, 2952))) < 
    ST_Distance(ST_EndPoint(ST_Transform(ind_line_geom, 2952)), ST_StartPoint(ST_Transform(line_geom_cut, 2952)))
THEN ST_LineSubstring(ind_line_geom, ST_LineLocatePoint(ind_line_geom, ST_StartPoint(line_geom_cut)), 1)  

ELSE NULL

END);

RAISE NOTICE 'Centrelines are now separated into their respective geo_id row. combined_section: %, section: %',
combined_section, section;

RETURN QUERY
SELECT int1, geo_id, lf_name, line_geom_separated, new_line, section, 
oid1_geom, oid1_geom_translated, objectid, fcode, fcode_desc, lev_sum, dist_from_pt, dist_from_translated_pt ;

DROP TABLE _wip;

END;
$BODY$;

