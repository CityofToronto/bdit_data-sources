--DROP MATERIALIZED VIEW gis_core.centreline_leg_directions;

CREATE MATERIALIZED VIEW gis_core.centreline_leg_directions AS

WITH toronto_cardinal (d, leg_label) AS (
    -- define cardinal directions in degrees, but rotated
    -- by -17 degrees to match the orientation of the street grid
    VALUES
    (360 - 17, 'north'),
    (90 - 17, 'east'),
    (180 - 17, 'south'),
    (270 - 17, 'west')
),

node_edges AS (
    -- Identify all connections between nodes and edges
    -- remove expressways because they don't *actually* intersect other streets
    SELECT
        centreline_id AS edge_id,
        from_intersection_id AS node_id
    FROM gis_core.centreline_latest
    WHERE feature_code_desc != 'Expressway'

    UNION

    SELECT
        centreline_id AS edge_id,
        to_intersection_id AS node_id
    FROM gis_core.centreline_latest
    WHERE feature_code_desc != 'Expressway'
),

nodes AS (
    -- find nodes with a degree > 2
    -- i.e. a legit intersection with three or more legs
    SELECT node_id
    FROM node_edges
    GROUP BY node_id
    HAVING COUNT(DISTINCT edge_id) > 2
),

legs AS (
    /*
    In this step we find the legs of all the intersections
    (either inbound or outbound) and measure their bearing
    or compass direction of travel *away* from the reference
    intersection. We also gather a partial "stub" geometry
    to represent the leg.
    */
    SELECT
        n.node_id AS intersection_centreline_id,
        edges_outbound.centreline_id AS leg_centreline_id,
        ST_Reverse(ST_LineSubstring(
            edges_outbound.geom,
            0, -- from start
            LEAST(
                30 / ST_Length(edges_outbound.geom::geography), -- fraction at 30m
                1 -- at most, the whole geometry
            )
        )) AS stub_geom,
        ST_Reverse(edges_outbound.geom) AS full_geom,
        degrees(ST_Azimuth(
            -- first and second points of the line
            ST_PointN(edges_outbound.geom, 1)::geography,
            ST_PointN(edges_outbound.geom, 2)::geography
        )) AS azimuth
    FROM nodes AS n
    JOIN gis_core.centreline_latest AS edges_outbound
        ON n.node_id = edges_outbound.from_intersection_id

    UNION

    SELECT
        n.node_id AS intersection_centreline_id,
        edges_inbound.centreline_id AS leg_centreline_id,
        ST_LineSubstring(
            edges_inbound.geom,
            GREATEST(
                1 - 30 / ST_Length(edges_inbound.geom::geography), -- fraction at 30m
                0 -- at most, the whole geometry
            ),
            1 -- to end
        ) AS stub_geom,
        edges_inbound.geom AS full_geom,
        degrees(ST_Azimuth(
            -- last and second to last points of the line
            -- (reverse of the above)
            ST_PointN(edges_inbound.geom, -1)::geography,
            ST_PointN(edges_inbound.geom, -2)::geography
        )) AS azimuth
    FROM nodes AS n
    JOIN gis_core.centreline_latest AS edges_inbound
        ON n.node_id = edges_inbound.to_intersection_id
),

distances AS (
    /*
    The cross-join here computes the angular distance between each
    of the cardinal directions and each of the legs.
    The next steps will try to select the pairs that minimize the
    total distance.
    */
    SELECT
        legs.intersection_centreline_id,
        legs.leg_centreline_id,
        toronto_cardinal.leg_label,
        -- *MATH*
        abs(180 - abs((toronto_cardinal.d - legs.azimuth)::numeric % 360 - 180)) AS angular_distance,
        legs.stub_geom,
        legs.full_geom
    FROM legs
    CROSS JOIN toronto_cardinal
),

leg1 AS (
    /*
    First, for each intersection, find the leg that best
    aligns with any of the cardinal directions.
    */
    SELECT DISTINCT ON (intersection_centreline_id)
        intersection_centreline_id,
        leg_centreline_id,
        leg_label,
        angular_distance,
        stub_geom,
        full_geom
    FROM distances
    ORDER BY
        intersection_centreline_id,
        angular_distance ASC
),

leg2 AS (
    /*
    Next, find the second best match - the best match after
    dropping out the cardinal direction which was assigned
    on the first pass
    */
    SELECT DISTINCT ON (intersection_centreline_id)
        intersection_centreline_id,
        leg_centreline_id,
        leg_label,
        angular_distance,
        stub_geom,
        full_geom
    FROM distances
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM leg1
            WHERE
                distances.intersection_centreline_id = leg1.intersection_centreline_id
                AND distances.leg_label = leg1.leg_label
        )
    ORDER BY
        intersection_centreline_id,
        angular_distance ASC
),

leg3 AS (
    /*
    Next, find the third best match - the best match after
    dropping out the cardinal directions which were assigned
    on the first or second pass
    */
    SELECT DISTINCT ON (intersection_centreline_id)
        intersection_centreline_id,
        leg_centreline_id,
        leg_label,
        angular_distance,
        stub_geom,
        full_geom
    FROM distances
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM leg1
            WHERE
                distances.intersection_centreline_id = leg1.intersection_centreline_id
                AND distances.leg_label = leg1.leg_label
        )
        AND NOT EXISTS (
            SELECT 1
            FROM leg2
            WHERE
                distances.intersection_centreline_id = leg2.intersection_centreline_id
                AND distances.leg_label = leg2.leg_label
        )
    ORDER BY
        intersection_centreline_id,
        angular_distance ASC
),

leg4 AS (
    /*
    Find the leg, if any, that is the closest match to the last
    remaining cardinal direction
    */
    SELECT DISTINCT ON (intersection_centreline_id)
        intersection_centreline_id,
        leg_centreline_id,
        leg_label,
        angular_distance,
        stub_geom,
        full_geom
    FROM distances
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM leg1
            WHERE
                distances.intersection_centreline_id = leg1.intersection_centreline_id
                AND distances.leg_label = leg1.leg_label
        )
        AND NOT EXISTS (
            SELECT 1
            FROM leg2
            WHERE
                distances.intersection_centreline_id = leg2.intersection_centreline_id
                AND distances.leg_label = leg2.leg_label
        )
        AND NOT EXISTS (
            SELECT 1
            FROM leg3
            WHERE
                distances.intersection_centreline_id = leg3.intersection_centreline_id
                AND distances.leg_label = leg3.leg_label
        )
    ORDER BY
        intersection_centreline_id,
        angular_distance ASC
),

unified_legs AS (
    -- Union the results of the four passes
    SELECT * FROM leg1
    UNION
    SELECT * FROM leg2
    UNION
    SELECT * FROM leg3
    UNION
    SELECT * FROM leg4
)

/*
The final select adds names from the edge table, and also removes
(via DISTINCT ON) legs assigned to more than one direction
(this will happen in the fourth pass for intersections with 3 legs).
*/
SELECT DISTINCT ON (
    unified_legs.intersection_centreline_id,
    unified_legs.leg_centreline_id
)
    unified_legs.intersection_centreline_id,
    unified_legs.leg_centreline_id,
    unified_legs.leg_label AS leg,
    -- last node of the leg is the intersection
    ST_PointN(unified_legs.stub_geom, -1) AS intersection_geom,
    edges.linear_name_full_legal AS street_name,
    unified_legs.stub_geom AS leg_stub_geom,
    unified_legs.full_geom AS leg_full_geom
FROM unified_legs
JOIN gis_core.centreline_latest AS edges
    ON unified_legs.leg_centreline_id = edges.centreline_id
ORDER BY
    unified_legs.intersection_centreline_id,
    unified_legs.leg_centreline_id,
    -- take the best match of any repeatedly assigned legs
    unified_legs.angular_distance ASC;

ALTER MATERIALIZED VIEW gis_core.centreline_leg_directions OWNER TO gis_admins;;

CREATE UNIQUE INDEX ON gis_core.centreline_leg_directions (intersection_centreline_id, leg_centreline_id);
CREATE INDEX ON gis_core.centreline_leg_directions USING GIST (leg_full_geom);
CREATE INDEX ON gis_core.centreline_leg_directions USING GIST (leg_stub_geom);

CREATE INDEX ON gis_core.centreline_leg_directions (intersection_centreline_id);

COMMENT ON MATERIALIZED VIEW gis_core.centreline_leg_directions
IS 'Automated mapping of centreline intersection legs onto the four cardinal directions. Please report any issues/inconsistencies with this view here: https://github.com/CityofToronto/bdit_data-sources/issues/1190';

COMMENT ON COLUMN gis_core.centreline_leg_directions.leg
IS 'cardinal direction, one of (north, east, south, west)';

COMMENT ON COLUMN gis_core.centreline_leg_directions.leg_stub_geom
IS 'first (up to) 30m of the centreline segment geometry pointing *inbound* toward the reference intersection';

COMMENT ON COLUMN gis_core.centreline_leg_directions.leg_full_geom
IS 'complete geometry of the centreline segment geometry, pointing *inbound* toward the reference intersection';
