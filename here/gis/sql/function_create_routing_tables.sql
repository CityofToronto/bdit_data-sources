-- For yearly maintenance
-- Create routing tables when we get a new map version
CREATE OR REPLACE FUNCTION here.create_rounting_tables(ref_yr text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE STRICT SECURITY DEFINER PARALLEL UNSAFE
AS $BODY$

DECLARE 
	_nodes TEXT := 'routing_nodes_'||ref_yr;
	_zlevel TEXT := 'zlevels_'||ref_yr;
	_traffic TEXT := 'traffic_streets_'||ref_yr;
	_streets_att TEXT := 'streets_att_'||ref_yr;
	_streets TEXT := 'routing_streets_'||ref_yr;
	_st TEXT := 'streets_'||ref_yr;

BEGIN
EXECUTE format($$
			   -- Create routing_nodes
			   CREATE VIEW here.%I AS
				SELECT node_id, rank() OVER(order by node_id, z_level) as vertex_id, link_id, geom
				FROM here_gis.%I
				WHERE intrsect = 'Y';
			   	ALTER TABLE here.%I OWNER TO here_admins;
			   -- Create traffic streets
			   CREATE MATERIALIZED VIEW IF NOT EXISTS here_gis.%I AS
				 SELECT streets_att.*
				 FROM here_gis.%I streets_att
				 WHERE 
					streets_att.paved::text = 'Y'::text  
					AND streets_att.poiaccess::text = 'N'::text  
					AND streets_att.ar_auto::text = 'Y'::text  
					AND streets_att.ar_traff::text = 'Y'::text;
				ALTER TABLE IF EXISTS here_gis.%I OWNER TO here_admins;
				GRANT SELECT ON TABLE here_gis.%I TO bdit_humans;
				GRANT ALL ON TABLE here_gis.%I TO here_admins;
			   -- Create routing streets
			   CREATE MATERIALIZED VIEW IF NOT EXISTS here.%I AS
							 SELECT streets.link_dir,
								streets.link_id,
								streets.id,
								streets.source,
								streets.source_vertex,
								streets.target,
								streets.target_vertex,
								streets.length,
								streets.geom,
								row_number() OVER (ORDER BY streets.id) AS edge_id
							   FROM ( SELECT traffic_streets.link_id || 'F'::text AS link_dir,
										traffic_streets.link_id,
										(to_char(traffic_streets.link_id, '0000000000'::text) || '0'::text)::bigint AS id,
										traffic_streets.ref_in_id AS source,
										sources.vertex_id AS source_vertex,
										traffic_streets.nref_in_id AS target,
										targets.vertex_id AS target_vertex,
										st_length(st_transform(streets_1.geom, 2952)) AS length,
										streets_1.geom
									   FROM here_gis.%I traffic_streets
										 JOIN here_gis.%I streets_1 USING (link_id)
										 JOIN here.%I sources USING (link_id)
										 JOIN here.%I targets USING (link_id)
									  WHERE traffic_streets.ref_in_id = sources.node_id AND traffic_streets.nref_in_id = targets.node_id AND (traffic_streets.dir_travel::text = ANY (ARRAY['F'::character varying::text, 'B'::character varying::text]))
									UNION ALL
									 SELECT traffic_streets.link_id || 'T'::text AS link_dir,
										traffic_streets.link_id,
										(to_char(traffic_streets.link_id, '0000000000'::text) || '1'::text)::bigint AS id,
										traffic_streets.nref_in_id AS source,
										sources.vertex_id AS source_vertex,
										traffic_streets.ref_in_id AS target,
										targets.vertex_id AS target_vertex,
										st_length(st_transform(streets_1.geom, 2952)) AS length,
										st_reverse(streets_1.geom) AS geom
									   FROM here_gis.%I traffic_streets
										 JOIN here_gis.%I streets_1 USING (link_id)
										 JOIN here.%I sources USING (link_id)
										 JOIN here.%I targets USING (link_id)
									  WHERE traffic_streets.nref_in_id = sources.node_id AND traffic_streets.ref_in_id = targets.node_id AND (traffic_streets.dir_travel::text = ANY (ARRAY['T'::character varying::text, 'B'::character varying::text]))) streets
							;

							ALTER TABLE IF EXISTS here.%I OWNER TO here_admins;

							GRANT SELECT ON TABLE here.%I TO bdit_humans;
							GRANT SELECT ON TABLE here.%I TO covid_admins;
							GRANT ALL ON TABLE here.%I TO here_admins;
							GRANT SELECT ON TABLE here.%I TO tt_request_bot;
                        $$
              , _nodes, _zlevel, _nodes, _traffic, _streets_att, _traffic, _traffic, _traffic, 
			  _streets, _traffic, _st, _nodes, _nodes, _traffic, _st, _nodes, _nodes,
			  _streets, _streets, _streets, _streets, _streets);
						
END;
$BODY$;						
						
ALTER FUNCTION here.create_rounting_tables(text)
    OWNER TO here_admins;
    
COMMENT ON FUNCTION here.create_rounting_tables(text)
    IS 'This function creates new (1) routing_nodes, (2) traffic_streets, and (3) routing_streets for the inputted map version. Run once during map version upgrade each year.';
					