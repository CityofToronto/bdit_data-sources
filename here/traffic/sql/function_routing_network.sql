/*Function to produce a network routable by pg_routing, of HERE traffic analytics data for a given 5-min bin.*/

CREATE OR REPLACE FUNCTION here.get_network_for_tx (_tx TIMESTAMP)
RETURNS TABLE (link_dir TEXT, source numeric, target numeric, cost numeric)
AS $$
 SELECT routing_streets.link_dir,
    routing_streets.source,
    routing_streets.target,
    3600.0 * ta.length::numeric / (1000 *     COALESCE(ta.pct_50, ref_spds.pattern_speed))::numeric AS cost
   FROM here.routing_streets
   LEFT OUTER JOIN here.ta ON routing_streets.link_dir = ta.link_dir AND tx = _tx
   INNER JOIN here.traffic_pattern_18_spd_15min ref_spds ON ref_spds.link_dir = routing_streets.link_dir 
   WHERE _tx::TIME <@ ref_spds.trange AND EXTRACT(isodow FROM _tx)::INT = ref_spds.isodow;
$$
LANGUAGE SQL STRICT STABLE;
GRANT EXECUTE ON FUNCTION here.get_network_for_tx (TIMESTAMP) TO bdit_humans;