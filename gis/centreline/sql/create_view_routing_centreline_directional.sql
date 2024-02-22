CREATE OR REPLACE VIEW gis_core.routing_centreline_directional AS
    
SELECT 
    centreline.centreline_id,
    concat(centreline.centreline_id, 0)::integer AS id,
    centreline.from_intersection_id AS source,
    centreline.to_intersection_id AS target,
    centreline.shape_length AS cost,
    centreline.geom
   FROM gis_core.centreline_latest centreline

UNION

SELECT 
    centreline.centreline_id,
    concat(centreline.centreline_id, 1)::integer AS id,
    centreline.to_intersection_id AS source,
    centreline.from_intersection_id AS target,
    centreline.shape_length AS cost,
    st_reverse(centreline.geom) AS geom
   FROM gis_core.centreline_latest centreline
  WHERE centreline.oneway_dir_code = 0;

ALTER TABLE gis_core.routing_centreline_directional
    OWNER TO gis_admins;

COMMENT ON VIEW gis_core.routing_centreline_directional
    IS 'A view that contains centreline streets for routing, with duplicated rows for two-way streets and flipped geometries when necessary. A new id has been assigned to each centreline to distinguish duplicated lines.';

GRANT SELECT ON TABLE gis_core.routing_centreline_directional TO bdit_humans;
GRANT ALL ON TABLE gis_core.routing_centreline_directional TO gis_admins;



