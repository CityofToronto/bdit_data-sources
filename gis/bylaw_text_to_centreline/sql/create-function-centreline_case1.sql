DROP FUNCTION jchew._centreline_case1_updated(text, text, text, double precision);
CREATE OR REPLACE FUNCTION jchew._centreline_case1_updated(highway2 TEXT, btwn2 TEXT, direction_btwn2 TEXT, metres_btwn2 FLOAT)
RETURNS TABLE(int1 INTEGER, geo_id NUMERIC, lf_name VARCHAR, line_geom GEOMETRY,
oid1_geom GEOMETRY, oid1_geom_translated GEOMETRY, objectid NUMERIC, fcode INTEGER, fcode_desc VARCHAR, lev_sum INTEGER,dist_from_pt FLOAT, dist_from_translated_pt FLOAT)
/*MAIN FUNCTION RETURNS (int1 INTEGER, int2 INTEGER, geo_id NUMERIC, lf_name VARCHAR, con TEXT, note TEXT, 
line_geom GEOMETRY, oid1_geom GEOMETRY, oid1_geom_translated GEOMETRY, oid2_geom geometry, oid2_geom_translated GEOMETRY, 
objectid NUMERIC, fcode INTEGER, fcode_desc VARCHAR)*/
LANGUAGE 'plpgsql'
AS 
$BODY$

BEGIN
RETURN QUERY
WITH X AS
(SELECT oid_geom AS oid1_geom, oid_geom_translated AS oid1_geom_translated, int_id_found AS int1, get_geom.lev_sum
FROM jchew._get_intersection_geom_updated(highway2, btwn2, direction_btwn2, metres_btwn2, 0) get_geom)
, Y AS (
SELECT *, 
ST_Distance(ST_Transform(a.oid1_geom,2952), ST_Transform(a.geom,2952)) AS dist_from_pt,
ST_Distance(ST_Transform(a.oid1_geom_translated,2952), ST_Transform(a.geom,2952)) AS dist_from_translated_pt
FROM 
(SELECT cl.geo_id, cl.lf_name, cl.objectid, cl.fcode, cl.fcode_desc, cl.geom, X.oid1_geom, X.oid1_geom_translated,
ST_DWithin(ST_Transform(cl.geom, 2952), 
		   ST_BUFFER(ST_MakeLine(ST_Transform(X.oid1_geom,2952), ST_Transform(X.oid1_geom_translated,2952)), metres_btwn2, 'endcap=flat join=round'),
		   1) AS dwithin
FROM gis.centreline cl, X
WHERE ST_DWithin(ST_Transform(cl.geom, 2952), 
		   ST_BUFFER(ST_MakeLine(ST_Transform(X.oid1_geom,2952), ST_Transform(X.oid1_geom_translated,2952)), metres_btwn2, 'endcap=flat join=round'),
		   1) = TRUE 
AND ST_Length(st_intersection(ST_BUFFER(ST_MakeLine(ST_Transform(X.oid1_geom,2952), ST_Transform(X.oid1_geom_translated,2952)), 3*(ST_LENGTH(ST_MakeLine(ST_Transform(X.oid1_geom,2952), ST_Transform(X.oid1_geom_translated,2952)))), 'endcap=flat join=round') , ST_Transform(cl.geom, 2952))) /ST_Length(ST_Transform(cl.geom, 2952)) > 0.9
) a
WHERE a.lf_name = highway2 
--AND ST_Distance(ST_Transform(a.oid1_geom,2952), ST_Transform(a.geom,2952)) != 0 
--ORDER BY dist
--LIMIT 1; OR USE MIN (dist)
)

SELECT X.int1, Y.geo_id, Y.lf_name, Y.geom AS line_geom, X.oid1_geom, X.oid1_geom_translated, Y.objectid, Y.fcode, Y.fcode_desc, 
X.lev_sum, Y.dist_from_pt, Y.dist_from_translated_pt
FROM X, Y;

END;
$BODY$;


******TESTING (IT WORKED!!!)
WITH X AS
(SELECT oid_geom AS oid1_geom, oid_geom_translated AS oid1_geom_translated, int_id_found AS int1, get_geom.lev_sum
FROM jchew._get_intersection_geom_updated('Glenwood Cres', 'O''Connor Dr', 'west', 330.33, 0) get_geom)
, Y AS (
SELECT *, 
ST_Distance(ST_Transform('0101000020E6100000F511CD4333D453C09E415F54F4D94540'::geometry,2952), ST_Transform(a.geom,2952)) AS dist_from_pt,
ST_Distance(ST_Transform('0101000020E61000006197236A76D453C025D8358BF4D94540'::geometry,2952), ST_Transform(a.geom,2952)) AS dist_from_translated_pt
FROM 
(SELECT cl.geo_id, cl.lf_name, cl.objectid, cl.fcode, cl.fcode_desc, cl.geom, 
ST_DWithin(ST_Transform(cl.geom, 2952), 
		   ST_BUFFER(ST_MakeLine(ST_Transform('0101000020E6100000F511CD4333D453C09E415F54F4D94540'::geometry,2952), ST_Transform('0101000020E61000006197236A76D453C025D8358BF4D94540'::geometry,2952)), 330.33, 'endcap=flat join=round'),
		   1) AS dwithin
FROM gis.centreline cl, X
WHERE ST_DWithin(ST_Transform(cl.geom, 2952), 
		   ST_BUFFER(ST_MakeLine(ST_Transform('0101000020E6100000F511CD4333D453C09E415F54F4D94540'::geometry,2952), ST_Transform('0101000020E61000006197236A76D453C025D8358BF4D94540'::geometry,2952)), 330.33, 'endcap=flat join=round'),
		   1) = TRUE 
           AND ST_Length(st_intersection(ST_BUFFER(ST_MakeLine(ST_Transform('0101000020E6100000F511CD4333D453C09E415F54F4D94540'::geometry,2952), ST_Transform('0101000020E61000006197236A76D453C025D8358BF4D94540'::geometry,2952)), 3*(ST_LENGTH(ST_MakeLine(ST_Transform('0101000020E6100000F511CD4333D453C09E415F54F4D94540'::geometry,2952), ST_Transform('0101000020E61000006197236A76D453C025D8358BF4D94540'::geometry,2952)))), 'endcap=flat join=round') , ST_Transform(cl.geom, 2952))) /ST_Length(ST_Transform(cl.geom, 2952)) > 0.9
           ) a
WHERE a.lf_name = 'Glenwood Cres' 
AND ST_Distance(ST_Transform('0101000020E6100000F511CD4333D453C09E415F54F4D94540'::geometry,2952), ST_Transform(a.geom,2952)) != 0 
--ORDER BY dist
--LIMIT 1;
)

SELECT X.int1, Y.geo_id, Y.lf_name, Y.geom AS line_geom, X.oid1_geom, X.oid1_geom_translated, Y.objectid, Y.fcode, Y.fcode_desc, X.lev_sum
FROM X, Y;




*******OLD ONE
CREATE OR REPLACE FUNCTION gis._centreline_case1(direction_btwn2 text, metres_btwn2 FLOAT, centreline_geom geometry, line_geom geometry, oid1_geom geometry)
RETURNS geometry AS $geom$
--from main function: 
--gis._centreline_case1(direction_btwn2, metres_btwn2, centreline_geom= ST_MakeLine(ST_LineMerge(match_line_to_centreline_geom)), line_geom,
--					oid1_geom= ST_GeomFromText((gis._get_intersection_geom(highway2, btwn1, NULL::TEXT, NULL::FLOAT, 0))[1], 2952) )

-- i.e. St Mark's Ave and a point 100 m north

DECLARE geom geometry := (
-- case where the section of street from the intersection in the specified direction is shorter than x metres
CASE WHEN metres_btwn2 > ST_Length(centreline_geom) AND metres_btwn2 - ST_Length(centreline_geom) < 15
THEN centreline_geom


-- metres_btwn2/ST_Length(d.geom) is the fraction that is supposed to be cut off from the dissolved centreline segment(s)
-- cut off the first fraction of the dissolved line, and the second and check to see which one is closer to the original interseciton

WHEN ST_LineLocatePoint(centreline_geom, oid1_geom)
> ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
THEN ST_LineSubstring(centreline_geom, ST_LineLocatePoint(centreline_geom, oid1_geom) - (metres_btwn2/ST_Length(centreline_geom)),
ST_LineLocatePoint(centreline_geom, oid1_geom))


WHEN ST_LineLocatePoint(centreline_geom, oid1_geom) <
ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1)))
-- take the substring from the intersection to the point x metres ahead of it
THEN ST_LineSubstring(centreline_geom, ST_LineLocatePoint(centreline_geom, oid1_geom),
ST_LineLocatePoint(centreline_geom, oid1_geom) + (metres_btwn2/ST_Length(centreline_geom))  )



END
);

BEGIN

raise notice 'IN CASE 1 FUNCTION !!!!!!!!!!!!!!!!!!!  
direction_btwn2: %, metres_btwn2: %  centreline_geom: %  line_geom: %  
oid1_geom: % llp1: %  llp2: % len centreline geom: %', 
direction_btwn2, metres_btwn2, ST_ASText(ST_Transform(centreline_geom, 4326)), ST_AsText(ST_Transform(line_geom, 4326)), 
ST_AsText(ST_Transform(oid1_geom, 4326)), ST_LineLocatePoint(centreline_geom, oid1_geom),  
ST_LineLocatePoint(centreline_geom, ST_ClosestPoint(centreline_geom, ST_LineSubstring(line_geom, 0.99999, 1))), 
ST_Length(centreline_geom);

RETURN geom;

END;
$geom$ LANGUAGE plpgSQL;


COMMENT ON FUNCTION gis._centreline_case1(text, FLOAT, geometry,geometry, geometry) IS '
Meant to split line geometries of bylaw in effect locations where the bylaw occurs between an intersection and an offset.
Check out README in https://github.com/CityofToronto/bdit_data-sources/tree/master/gis/bylaw_text_to_centreline for more information';
