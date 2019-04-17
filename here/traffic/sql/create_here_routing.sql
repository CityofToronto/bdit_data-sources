/*
Create a set of links compatible with pg_routing.
Need to include length in meters from the geometry in case of gap-filling.
The cost will come from traffic analytics data which contains both speed.
*/


/*Filter to only include intersections and reset index*/
DROP VIEW IF EXISTS here.routing_nodes CASCADE;
CREATE VIEW here.routing_nodes AS
SELECT node_id, rank() OVER(order by node_id, z_level) as vertex_id, link_id, geom
	FROM here_gis.zlevels_18_3
	WHERE intrsect = 'Y';

-- View: here.routing_streets_18_3

DROP MATERIALIZED VIEW here.routing_streets_18_3 CASCADE;

CREATE MATERIALIZED VIEW here.routing_streets_18_3
TABLESPACE pg_default
AS
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
   FROM ( SELECT traffic_streets_18_3.link_id || 'F'::text AS link_dir,
            traffic_streets_18_3.link_id,
            (to_char(traffic_streets_18_3.link_id, '0000000000'::text) || '0'::text)::bigint AS id,
            traffic_streets_18_3.ref_in_id AS source,
            sources.vertex_id AS source_vertex,
            traffic_streets_18_3.nref_in_id AS target,
            targets.vertex_id AS target_vertex,
            st_length(st_transform(streets_18_3.geom, 3857)) AS length,
            streets_18_3.geom
           FROM here_gis.traffic_streets_18_3
             JOIN here_gis.streets_18_3 USING (link_id)
             JOIN here.routing_nodes sources USING (link_id)
             JOIN here.routing_nodes targets USING (link_id)
          WHERE traffic_streets_18_3.ref_in_id = sources.node_id AND traffic_streets_18_3.nref_in_id = targets.node_id AND (traffic_streets_18_3.dir_travel::text = ANY (ARRAY['F'::character varying::text, 'B'::character varying::text]))
        UNION ALL
         SELECT traffic_streets_18_3.link_id || 'T'::text AS link_dir,
            traffic_streets_18_3.link_id,
            (to_char(traffic_streets_18_3.link_id, '0000000000'::text) || '1'::text)::bigint AS id,
            traffic_streets_18_3.nref_in_id AS source,
            sources.vertex_id AS source_vertex,
            traffic_streets_18_3.ref_in_id AS target,
            targets.vertex_id AS target_vertex,
            st_length(st_transform(streets_18_3.geom, 3857)) AS length,
            st_reverse(streets_18_3.geom) AS geom
           FROM here_gis.traffic_streets_18_3
             JOIN here_gis.streets_18_3 USING (link_id)
             JOIN here.routing_nodes sources USING (link_id)
             JOIN here.routing_nodes targets USING (link_id)
          WHERE traffic_streets_18_3.nref_in_id = sources.node_id AND traffic_streets_18_3.ref_in_id = targets.node_id AND (traffic_streets_18_3.dir_travel::text = ANY (ARRAY['T'::character varying::text, 'B'::character varying::text]))) streets
WITH DATA;

ALTER TABLE here.routing_streets_18_3
    OWNER TO rdumas;

GRANT ALL ON TABLE here.routing_streets_18_3 TO rdumas;
GRANT SELECT ON TABLE here.routing_streets_18_3 TO bdit_humans WITH GRANT OPTION;

CREATE INDEX routing_streets_18_3_link_dir_idx
    ON here.routing_streets_18_3 USING btree
    (link_dir COLLATE pg_catalog."default")
    TABLESPACE pg_default;
CREATE INDEX routing_streets_18_3_link_id_idx
    ON here.routing_streets_18_3 USING btree
    (link_id)
    TABLESPACE pg_default;
