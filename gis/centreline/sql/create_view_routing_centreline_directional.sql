CREATE OR REPLACE VIEW gis_core.routing_centreline_directional AS

SELECT 
    centreline_id,
    concat(row_number() OVER (), dir)::bigint AS id,
    source,
    target,
    cost,
    geom

FROM (
    SELECT
        centreline.centreline_id,
        centreline.from_intersection_id AS source,
        centreline.to_intersection_id AS target,
        centreline.shape_length AS cost,
        centreline.geom,
        0 as dir
    FROM gis_core.centreline_latest AS centreline

    UNION

    SELECT
        centreline.centreline_id,
        centreline.to_intersection_id AS source,
        centreline.from_intersection_id AS target,
        centreline.shape_length AS cost,
        st_reverse(centreline.geom) AS geom,
        1 as dir
    FROM gis_core.centreline_latest AS centreline
    WHERE centreline.oneway_dir_code = 0) AS dup;

ALTER TABLE gis_core.routing_centreline_directional OWNER TO gis_admins;

COMMENT ON VIEW gis_core.routing_centreline_directional
IS 'A view that contains centreline streets for routing, with duplicated rows for two-way streets and flipped geometries when necessary. A new id has been assigned to each centreline to distinguish duplicated lines.';

GRANT SELECT ON TABLE gis_core.routing_centreline_directional TO bdit_humans;
GRANT ALL ON TABLE gis_core.routing_centreline_directional TO gis_admins;



