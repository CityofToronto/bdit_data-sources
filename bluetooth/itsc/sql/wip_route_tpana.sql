/*A work in progress script to route TPANA routes between start and end nodes along the centreline.
Runs in around 3 minutes for 200 routes, but many of them are not matched to a centreline route. More work is required. */

WITH routes AS (
    SELECT *
    FROM bluetooth.itsc_tt_paths
    WHERE
        division_id = 8026 --TPANA routes do not have proper paths drawn.
        AND encoded_polyline IS NOT NULL
),

start_int AS (
    --find closest intersection_id to start_node
    SELECT DISTINCT ON (path_id, division_id, start_timestamp)
        n.path_id,
        n.division_id,
        n.start_timestamp,
        cip.intersection_id
    FROM routes AS n
    JOIN gis_core.centreline_intersection_point_latest AS cip ON st_dwithin(cip.geom, n.start_node, 0.002)
    ORDER BY
        n.path_id,
        n.division_id,
        n.start_timestamp,
        --pick the closest node
        cip.geom <-> n.start_node
),

end_int AS (
    --find closest intersection_id to end_node
    SELECT DISTINCT ON (path_id, division_id, start_timestamp)
        n.path_id,
        n.division_id,
        n.start_timestamp,
        cip.intersection_id
    FROM routes AS n
    JOIN gis_core.centreline_intersection_point_latest AS cip ON st_dwithin(cip.geom, n.end_node, 0.002)
    ORDER BY
        n.path_id,
        n.division_id,
        n.start_timestamp,
        --pick the closest node
        cip.geom <-> n.end_node
)

SELECT
    start_int.path_id,
    start_int.division_id,
    start_int.start_timestamp,
    lat.edges,
    lat.geom
FROM start_int
JOIN end_int USING (path_id, division_id, start_timestamp),
LATERAL (
    --route using only gardiner/lakeshore/DVP
    SELECT
        ARRAY_AGG(edge ORDER BY path_seq) AS edges,
        st_union(st_linemerge(routing_centreline_directional.geom) ORDER BY path_seq) AS geom
    FROM pgr_trsp('
        SELECT
            id,
            source::int,
            target::int,
            cost_length::int AS cost
        FROM gis_core.routing_centreline_directional
        INNER JOIN gis_core.centreline_latest USING (centreline_id)
        WHERE feature_code IN (201101, 201201, 201100, 201200)
            AND (
                linear_name_full ILIKE ''%Gardiner%''
                OR linear_name_full ILIKE ''%Lakeshore%''
                OR linear_name_full ILIKE ''%Lake Shore%''
                OR linear_name_full ILIKE ''%Don Valley%''
                OR linear_name_full ILIKE ''%DVP%''
            )',
        'SELECT path, cost FROM gis_core.centreline_routing_restrictions',
        start_int.intersection_id,
        end_int.intersection_id,
        true
    )
    JOIN gis_core.routing_centreline_directional ON edge = id
) AS lat;