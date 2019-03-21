/*
Create a set of links compatible with pg_routing.
Need to include length in meters from the geometry in case of gap-filling.
The cost will come from traffic analytics data which contains both speed.
*/

-- View: here.routing_streets_18_3

DROP MATERIALIZED VIEW here.routing_streets_18_3 CASCADE;

CREATE MATERIALIZED VIEW here.routing_streets_18_3
TABLESPACE pg_default
AS

/*Links in the FROM direction of travel*/
 SELECT traffic_streets_18_3.link_id || 'F'::text AS link_dir,
 	(to_char(traffic_streets_18_3.link_id, '0000000000') || '0')::BIGINT as id, /*pg_routing requires numeric ids*/
    traffic_streets_18_3.ref_in_id AS source,
    traffic_streets_18_3.nref_in_id AS target,
    ST_Length(ST_Transform(streets_18_3.geom, 3857)) as length,
	streets_18_3.geom
   FROM here_gis.traffic_streets_18_3
     JOIN here_gis.streets_18_3 USING (link_id)
  WHERE traffic_streets_18_3.dir_travel::text = ANY (ARRAY['F'::character varying::text, 'B'::character varying::text])
UNION ALL
/*Links in the TO direction of travel, need to duplicate because HERE links are unique to both directions of travel (`dir_travel`)*/
 SELECT traffic_streets_18_3.link_id || 'T'::text AS link_dir,
 	(to_char(traffic_streets_18_3.link_id, '0000000000') || '1')::BIGINT as id,
    traffic_streets_18_3.nref_in_id AS source,
    traffic_streets_18_3.ref_in_id AS target,
	ST_Length(ST_Transform(streets_18_3.geom, 3857)) as length,
    st_reverse(streets_18_3.geom) AS geom
   FROM here_gis.traffic_streets_18_3
     JOIN here_gis.streets_18_3 USING (link_id)
  WHERE traffic_streets_18_3.dir_travel::text = ANY (ARRAY['T'::character varying::text, 'B'::character varying::text])
WITH DATA;

ALTER TABLE here.routing_streets_18_3
    OWNER TO rdumas;

GRANT ALL ON TABLE here.routing_streets_18_3 TO rdumas;
GRANT SELECT ON TABLE here.routing_streets_18_3 TO bdit_humans WITH GRANT OPTION;

CREATE INDEX routing_streets_18_3_link_dir_idx
    ON here.routing_streets_18_3 USING btree
    (link_dir COLLATE pg_catalog."default")
    TABLESPACE pg_default;