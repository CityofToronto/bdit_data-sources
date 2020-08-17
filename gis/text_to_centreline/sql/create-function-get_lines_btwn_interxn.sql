--EXAMPLE: USING HERE network
--FOR HERE NETWORK AND NODES AS px_start and px_end
--Function to get link_dir from px_start and px_end
CREATE or REPLACE FUNCTION here_gis.get_links_btwn_px(px_start int, px_end int)
RETURNS TABLE (px_start int, px_end int, seq int, link_dir text)
AS $$
WITH input as (SELECT px_start as px_start, px_end as px_end)
,lookup as (
    SELECT px_start, px_end, origin.node_id as source, dest.node_id as target
    FROM input
    inner join here_gis.px_nodes origin on px_start = origin.px
    inner join here_gis.px_nodes dest on px_end = dest.px
)
, results as (SELECT * FROM
    lookup 
    cross join lateral pgr_dijkstra('SELECT id, source::int, target::int, length::int as cost from here.routing_streets_18_3', source::int, target::int)
)
SELECT px_start, px_end, seq, link_dir
from results
inner join here.routing_streets_18_3 on edge=id
order by px_start, px_end, seq
$$
LANGUAGE SQL STRICT STABLE;

--USING centrelines aka GIS network
-- DROP FUNCTION jchew.get_lines_btwn_interxn(text, integer, integer);
-- FUNCTION: jchew.get_lines_btwn_interxn(text, integer, integer)
CREATE OR REPLACE FUNCTION jchew.get_lines_btwn_interxn(
	_highway2 text,
	_int_start integer,
	_int_end integer)
    RETURNS TABLE(int_start integer, int_end integer, seq integer, geo_id numeric, lf_name character varying, objectid numeric, geom geometry, fcode integer, fcode_desc character varying) 
    LANGUAGE 'plpgsql'

    COST 100
    STABLE STRICT 
    ROWS 1000
AS $BODY$

BEGIN
RETURN QUERY

WITH 
results AS (SELECT _int_start, _int_end, * FROM
    pgr_dijkstra(format('SELECT id, source::int, target::int,
				 CASE WHEN levenshtein(TRIM(lf_name), TRIM(%L), 1, 1,1) < 3 THEN (0.3*cost)::float ELSE cost END AS cost 
				 from gis.centreline_routing_undirected_lfname'::TEXT, _highway2),
				 _int_start::bigint, _int_end::bigint, FALSE)
--or do pgr_dijkstra('SELECT id, source::int, target::int, 
	--CASE lf_name WHEN '''|| _highway2 ||''' THEN (0.3*cost)::float ELSE cost END AS cost from gis.centreline_routing_undirected_lfname'::TEXT, ... )
)
SELECT results._int_start, results._int_end, results.seq, 
centre.geo_id, centre.lf_name, centre.objectid, centre.geom, centre.fcode, centre.fcode_desc 
FROM results
INNER JOIN gis.centreline centre ON edge=centre.geo_id
--WHERE levenshtein(TRIM(centre.lf_name), TRIM(_highway2), 1, 1, 1) < 3
--instead of `WHERE centre.lf_name = _highway2` because the street name might not be EXACTLY the same 
--(or we can get the output of get_intersection_id for lf_name and input it here but lf_name is not an output for that function currently)
ORDER BY int_start, int_end, seq;

RAISE NOTICE 'pg_routing done for int_start: % and int_end: %', 
_int_start, _int_end;

END;
$BODY$;

--create directed gis centreline network for pg routing (NOT SURE IF WE ARE USING IT YET)
CREATE MATERIALIZED VIEW jchew.centreline_routing_dir as 
SELECT centreline_id AS id, from_intersection_id AS source, 
to_intersection_id AS target, shape_length::int AS cost
FROM gis.centreline_directional
