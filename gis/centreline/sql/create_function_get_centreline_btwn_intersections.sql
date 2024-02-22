CREATE OR REPLACE FUNCTION gis_core.get_centreline_btwn_intersections(
	_node_start integer,
	_node_end integer,
	OUT _node_start_out integer,
	OUT _node_end integer,
	OUT links text[],
	OUT geom geometry)
    RETURNS record
    LANGUAGE 'sql'
    COST 100
    STABLE STRICT PARALLEL UNSAFE
AS $BODY$
WITH results as (
	SELECT * 
	FROM pgr_dijkstra('SELECT id, source::int, target::int, cost::int 
					  as cost from gis_core.routing_centreline_directional', _node_start, _node_end)
)

SELECT _node_start, _node_end, array_agg(centreline_id), ST_union(ST_linemerge(geom)) as geom 
from results
inner join gis_core.routing_centreline_directional on edge=id

$BODY$;

ALTER FUNCTION gis_core.get_centreline_btwn_intersections(integer, integer)
    OWNER TO gis_admins;

COMMENT ON FUNCTION gis_core.get_centreline_btwn_intersections(integer, integer)
    IS 'Routing function for centreline, takes in start intersection_id and end intersection_id and returns an array of centreline_id, as well as one line geometry between two intersections.';

GRANT EXECUTE ON FUNCTION gis_core.get_centreline_btwn_intersections(integer, integer) TO bdit_humans;

GRANT EXECUTE ON FUNCTION gis_core.get_centreline_btwn_intersections(integer, integer) TO gis_admins;

