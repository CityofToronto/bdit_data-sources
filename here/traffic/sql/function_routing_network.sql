/*Function to produce a network routable by pg_routing, of HERE traffic analytics data for a given 5-min bin.*/
DROP FUNCTION here.get_network_for_tx(timestamp without time zone);

CREATE OR REPLACE FUNCTION here.get_network_for_tx (_tx TIMESTAMP)
RETURNS TABLE (id bigint, source int, target int, cost int)
AS $$
 SELECT routing_streets.id,
    routing_streets.source::INT,
    routing_streets.target::INT,
    (3600.0 * routing_streets.length::numeric / (1000 *     COALESCE(ta.pct_50, ref_spds.pattern_speed))::numeric)::INT AS cost
   FROM here.routing_streets_18_3 routing_streets
   LEFT OUTER JOIN here.ta ON routing_streets.link_dir = ta.link_dir AND tx = _tx
   INNER JOIN here.traffic_pattern_18_spd_15min ref_spds ON ref_spds.link_dir = routing_streets.link_dir 
   WHERE _tx::TIME <@ ref_spds.trange AND EXTRACT(isodow FROM _tx)::INT = ref_spds.isodow;
$$
LANGUAGE SQL STRICT STABLE;
GRANT EXECUTE ON FUNCTION here.get_network_for_tx (TIMESTAMP) TO bdit_humans;

DROP FUNCTION here.get_network_for_tx(timestamp without time zone, boolean);

CREATE OR REPLACE FUNCTION here.get_network_for_tx (_tx TIMESTAMP, shrunk_ids BOOLEAN default FALSE)
RETURNS TABLE (id bigint, source int, target int, cost int)
AS $$ 
BEGIN
	IF shrunk_ids THEN
		SELECT routing_streets.edge_id,
		routing_streets.source_vertex::INT,
		routing_streets.target_vertex::INT,
		(3600.0 * routing_streets.length::numeric / (1000 *     COALESCE(ta.pct_50, ref_spds.pattern_speed))::numeric)::INT AS cost
		FROM here.routing_streets_18_3 routing_streets
		LEFT OUTER JOIN here.ta ON routing_streets.link_dir = ta.link_dir AND tx = _tx
		INNER JOIN here.traffic_pattern_18_spd_15min ref_spds ON ref_spds.link_dir = routing_streets.link_dir 
		WHERE _tx::TIME <@ ref_spds.trange AND EXTRACT(isodow FROM _tx)::INT = ref_spds.isodow;
	ELSE 
				SELECT routing_streets.id,
		routing_streets.source::INT,
		routing_streets.target::INT,
		(3600.0 * routing_streets.length::numeric / (1000 *     COALESCE(ta.pct_50, ref_spds.pattern_speed))::numeric)::INT AS cost
		FROM here.routing_streets_18_3 routing_streets
		LEFT OUTER JOIN here.ta ON routing_streets.link_dir = ta.link_dir AND tx = _tx
		INNER JOIN here.traffic_pattern_18_spd_15min ref_spds ON ref_spds.link_dir = routing_streets.link_dir 
		WHERE _tx::TIME <@ ref_spds.trange AND EXTRACT(isodow FROM _tx)::INT = ref_spds.isodow;
	END IF;
END;
$$
LANGUAGE PLPGSQL STRICT STABLE;
GRANT EXECUTE ON FUNCTION here.get_network_for_tx (TIMESTAMP, boolean) TO bdit_humans;