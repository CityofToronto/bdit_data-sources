DROP FUNCTION jchew._centreline_case2_combined(text,text,text,double precision,text,text,double precision,text,text);
CREATE OR REPLACE FUNCTION jchew._centreline_case2_combined(
	highway2 text,
    btwn1 text,
    direction_btwn1 text, 
    metres_btwn1 double precision,
	btwn2 text,
	direction_btwn2 text,
	metres_btwn2 double precision,
    btwn2_orig text,
    btwn2_check text)
    RETURNS TABLE(int_start integer, int_end integer, seq integer, geo_id numeric, lf_name character varying, 
    ind_line_geom geometry, line_geom geometry, line_geom_cut geometry, 
    section numrange, combined_section numrange, 
    oid1_geom geometry, oid1_geom_translated geometry, oid2_geom geometry, oid2_geom_translated geometry, 
    objectid numeric, fcode integer, fcode_desc character varying, lev_sum integer)
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
    int1 INTEGER, 
    int2 INTEGER,
    seq INTEGER,
    geo_id NUMERIC, 
    lf_name VARCHAR, 
    ind_line_geom GEOMETRY,
    line_geom GEOMETRY, 
    new_line1 GEOMETRY,
    new_line2 GEOMETRY,
    section NUMRANGE,
    oid1_geom GEOMETRY, 
    oid1_geom_translated GEOMETRY, 
    oid2_geom GEOMETRY, 
    oid2_geom_translated GEOMETRY, 
    objectid NUMERIC, 
    fcode INTEGER, 
    fcode_desc VARCHAR, 
    lev_sum INTEGER, 
    line_geom_cut GEOMETRY,
    line_geom_reversed GEOMETRY,
    combined_section NUMRANGE,
    whole_centreline GEOMETRY
);

TRUNCATE TABLE _wip2; 

    _int1_result := jchew._get_intersection_geom_updated(highway2, btwn1, direction_btwn1, metres_btwn1, 0);

    _int2_result := (CASE WHEN btwn2_orig LIKE '%point%' AND (btwn2_check NOT LIKE '% of %' OR btwn2_check LIKE ('% of ' || TRIM(btwn1)))
                THEN jchew._get_intersection_geom_updated(highway2, btwn2, direction_btwn2, metres_btwn2, 0)
                ELSE jchew._get_intersection_geom_updated(highway2, btwn2, direction_btwn2, metres_btwn2, _int1_result.int_id_found)
                END);
                
    INSERT INTO _wip2(int1, int2, seq, geo_id, lf_name, ind_line_geom,
    oid1_geom, oid1_geom_translated, oid2_geom, oid2_geom_translated, objectid,	fcode, fcode_desc, lev_sum)
    SELECT rout.int_start, rout.int_end, rout.seq, rout.geo_id, rout.lf_name, geom AS ind_line_geom, 
    _int1_result.oid_geom AS oid1_geom, _int1_result.oid_geom_translated AS oid1_geom_translated,
    _int2_result.oid_geom AS oid2_geom, _int2_result.oid_geom_translated AS oid2_geom_translated,
    rout.objectid, rout.fcode, rout.fcode_desc, _int1_result.lev_sum + _int2_result.lev_sum
    FROM jchew.get_lines_btwn_interxn(highway2, _int1_result.int_id_found, _int2_result.int_id_found) rout;

--those centreline found from buffer are where int_start, int_end, seq, lev_sum = NULL
INSERT INTO _wip2 (geo_id, lf_name, ind_line_geom, new_line1, new_line2, 
oid1_geom, oid1_geom_translated, oid2_geom, oid2_geom_translated,
objectid, fcode, fcode_desc)

WITH get_int AS
(SELECT _wip2.lf_name, _wip2.oid1_geom, _wip2.oid1_geom_translated, 
_wip2.oid2_geom, _wip2.oid2_geom_translated, 
ST_MakeLine(_wip2.oid1_geom, _wip2.oid1_geom_translated) AS new_line1,
ST_MakeLine(_wip2.oid2_geom, _wip2.oid2_geom_translated) AS new_line2
FROM _wip2 
--The columns I want are all the same for each row anyway besides the centreline information ie geo_id
LIMIT 1) 
SELECT cl.geo_id, cl.lf_name, cl.geom, 
	get_int.new_line1, get_int.new_line2,
	get_int.oid1_geom, get_int.oid1_geom_translated, 
	get_int.oid2_geom, get_int.oid2_geom_translated, 
	cl.objectid, cl.fcode, cl.fcode_desc
FROM gis.centreline cl
INNER JOIN get_int USING (lf_name) --only get those with desired street names
WHERE cl.geo_id NOT IN (SELECT _wip2.geo_id FROM _wip2)  --not repeating those found from pgrouting
AND (ST_DWithin(ST_Transform(cl.geom, 2952), 
		   ST_BUFFER(ST_Transform(get_int.new_line1, 2952), 3*metres_btwn1, 'endcap=flat join=round'),
		   10) = TRUE 
	  OR
	   ST_DWithin(ST_Transform(cl.geom, 2952), 
		   ST_BUFFER(ST_Transform(get_int.new_line2, 2952), 3*metres_btwn2, 'endcap=flat join=round'),
		   10) = TRUE 
	   )
--as some centreline is much longer compared to the short road segment, the ratio is set to 0.1 instead of 0.9
AND (ST_Length(ST_Intersection(
	ST_Buffer(ST_Transform(get_int.new_line1, 2952), 3*(ST_Length(ST_Transform(get_int.new_line1, 2952))), 'endcap=flat join=round') , 
	ST_Transform(cl.geom, 2952))) / ST_Length(ST_Transform(cl.geom, 2952)) > 0.1 
	OR
	ST_Length(ST_Intersection(
	ST_Buffer(ST_Transform(get_int.new_line2, 2952), 3*(ST_Length(ST_Transform(get_int.new_line2, 2952))), 'endcap=flat join=round') , 
	ST_Transform(cl.geom, 2952))) / ST_Length(ST_Transform(cl.geom, 2952)) > 0.1
	 ) ;

RAISE NOTICE 'Centrelines within the buffer and have the same bylaws highway name is found.'; 


/*
RETURN QUERY
SELECT int1, int2, _wip2.seq, _wip2.geo_id, _wip2.lf_name, 
_wip2.ind_line_geom, _wip2.line_geom, _wip2.line_geom_cut,
_wip2.section, _wip2.combined_section, 
_wip2.oid1_geom, _wip2.oid1_geom_translated, _wip2.oid2_geom, _wip2.oid2_geom_translated, 
_wip2.objectid, _wip2.fcode, _wip2.fcode_desc, _wip2.lev_sum
FROM _wip2;

DROP TABLE _wip2;

END;
$BODY$;
*/

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
direction_btwn2: %, metres_btwn2: %  whole_centreline: %  line_geom: %',
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
SELECT _wip.int1, _wip.geo_id, _wip.lf_name, 
_wip.ind_line_geom, _wip.line_geom, _wip.line_geom_cut, 
_wip.section, _wip.combined_section,
_wip.oid1_geom, _wip.oid1_geom_translated, 
_wip.objectid, _wip.fcode, _wip.fcode_desc, _wip.lev_sum 
FROM _wip;

DROP TABLE _wip;

END;
$BODY$;

ALTER FUNCTION jchew._centreline_case1_combined(text, text, text, double precision)
    OWNER TO jchew;

