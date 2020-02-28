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
--DROP FUNCTION jchew.get_lines_btwn_interxn(integer, integer);
CREATE or REPLACE FUNCTION jchew.get_lines_btwn_interxn(_int_start int, _int_end int)
RETURNS TABLE (int_start int, int_end int, seq int, geo_id numeric, lf_name varchar, geom geometry)
LANGUAGE 'plpgsql' STRICT STABLE
AS $BODY$

BEGIN
RETURN QUERY
WITH 
results AS (SELECT _int_start, _int_end, * FROM
    pgr_dijkstra('SELECT id, source::int, target::int, cost from gis.centreline_routing_undirected', _int_start::int, _int_end::int, FALSE)
)
SELECT results._int_start, results._int_end, results.seq, centre.geo_id, centre.lf_name, centre.geom
FROM results
INNER JOIN gis.centreline centre ON edge=centre.geo_id
ORDER BY int_start, int_end, seq;

RAISE NOTICE 'pg_routing done for int_start: % and int_end: %', 
_int_start, _int_end;

END;
$BODY$;
