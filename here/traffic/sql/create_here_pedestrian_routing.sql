/*
Routing network for routing pedestrians through the here network
*/

-- View: here_gis.pedestrian_streets_19_4

-- DROP MATERIALIZED VIEW here_gis.pedestrian_streets_19_4;

CREATE MATERIALIZED VIEW here_gis.pedestrian_streets_19_4
TABLESPACE pg_default
AS
 SELECT streets_att_19_4.link_id,
    streets_att_19_4.st_name,
    streets_att_19_4.st_nm_suff,
    streets_att_19_4.ref_in_id,
    streets_att_19_4.nref_in_id,
    streets_att_19_4.func_class,
    streets_att_19_4.speed_cat,
    streets_att_19_4.fr_spd_lim,
    streets_att_19_4.to_spd_lim,
    streets_att_19_4.to_lanes,
    streets_att_19_4.from_lanes,
    streets_att_19_4.lane_cat,
    streets_att_19_4.divider,
    streets_att_19_4.dir_travel,
    streets_att_19_4.ar_auto,
    streets_att_19_4.ar_traff,
    streets_att_19_4.ar_deliv,
    streets_att_19_4.ar_emerveh,
    streets_att_19_4.paved,
    streets_att_19_4.private,
    streets_att_19_4.frontage,
    streets_att_19_4.bridge,
    streets_att_19_4.tunnel,
    streets_att_19_4.ramp,
    streets_att_19_4.poiaccess,
    streets_att_19_4.route_type
   FROM here_gis.streets_att_19_4
  WHERE ar_pedest = 'Y'
WITH DATA;

ALTER TABLE here_gis.pedestrian_streets_19_4
    OWNER TO rdumas;

GRANT ALL ON TABLE here_gis.pedestrian_streets_19_4 TO rdumas;
GRANT SELECT ON TABLE here_gis.pedestrian_streets_19_4 TO bdit_humans;
COMMENT ON MATERIALIZED VIEW here_gis.pedestrian_streets_19_4 IS 'Here streets filtered for pedestrian access';

CREATE INDEX pedestrian_streets_19_4_link_id_idx
    ON here_gis.pedestrian_streets_19_4 USING btree
    (link_id)
    TABLESPACE pg_default;

-- View: here.routing_streets_19_4_ped

-- DROP MATERIALIZED VIEW here.routing_streets_19_4_ped;

CREATE MATERIALIZED VIEW here.routing_streets_19_4_ped
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
   FROM ( SELECT pedestrian_streets_19_4.link_id || 'F'::text AS link_dir,
            pedestrian_streets_19_4.link_id,
            (to_char(pedestrian_streets_19_4.link_id, '0000000000'::text) || '0'::text)::bigint AS id,
            pedestrian_streets_19_4.ref_in_id AS source,
            sources.vertex_id AS source_vertex,
            pedestrian_streets_19_4.nref_in_id AS target,
            targets.vertex_id AS target_vertex,
            st_length(st_transform(streets_19_4.geom, 3857)) AS length,
            streets_19_4.geom
           FROM here_gis.pedestrian_streets_19_4
             JOIN here_gis.streets_19_4 USING (link_id)
             JOIN here.routing_nodes_19_4 sources USING (link_id)
             JOIN here.routing_nodes_19_4 targets USING (link_id)
          WHERE pedestrian_streets_19_4.ref_in_id::numeric = sources.node_id::numeric AND pedestrian_streets_19_4.nref_in_id::numeric = targets.node_id::numeric         UNION ALL
         SELECT pedestrian_streets_19_4.link_id || 'T'::text AS link_dir,
            pedestrian_streets_19_4.link_id,
            (to_char(pedestrian_streets_19_4.link_id, '0000000000'::text) || '1'::text)::bigint AS id,
            pedestrian_streets_19_4.nref_in_id AS source,
            sources.vertex_id AS source_vertex,
            pedestrian_streets_19_4.ref_in_id AS target,
            targets.vertex_id AS target_vertex,
            st_length(st_transform(streets_19_4.geom, 3857)) AS length,
            st_reverse(streets_19_4.geom) AS geom
           FROM here_gis.pedestrian_streets_19_4
             JOIN here_gis.streets_19_4 USING (link_id)
             JOIN here.routing_nodes_19_4 sources USING (link_id)
             JOIN here.routing_nodes_19_4 targets USING (link_id)
          WHERE pedestrian_streets_19_4.nref_in_id::numeric = sources.node_id::numeric AND pedestrian_streets_19_4.ref_in_id::numeric = targets.node_id::numeric ) streets
WITH DATA;

ALTER TABLE here.routing_streets_19_4_ped
    OWNER TO rdumas;

COMMENT ON MATERIALIZED VIEW here.routing_streets_19_4_ped IS 'Routing network using streets with pedestrian access';

GRANT ALL ON TABLE here.routing_streets_19_4_ped TO rdumas;
GRANT SELECT ON TABLE here.routing_streets_19_4_ped TO bdit_humans WITH GRANT OPTION;