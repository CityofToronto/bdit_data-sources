--DROP MATERIALIZED VIEW vz_intersection.lpi_centreline_leg_directions;

--CREATE MATERIALIZED VIEW vz_intersection.lpi_centreline_leg_directions AS

WITH toronto_cardinal (d, leg_label) AS (
    VALUES -- rotate compass by -17 degrees to match street grid
    (360 - 17, 'north'),
    (90 - 17, 'east'),
    (180 - 17, 'south'),
    (270 - 17, 'west')
),

node_edges AS (
    -- collect inbound and outbound edges
    SELECT
        centreline_id AS edge_id,
        from_intersection_id AS node_id
    FROM gis_core.centreline_latest

    UNION

    SELECT
        centreline_id AS edge_id,
        to_intersection_id AS node_id
    FROM gis_core.centreline_latest
),

nodes AS (
    SELECT
        node_id,
        COUNT(DISTINCT edge_id) AS degree
    FROM node_edges
    GROUP BY node_id
    -- only where it's a legit "intersection"
    HAVING COUNT(DISTINCT edge_id) > 2
),

legs AS (
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
        degrees(ST_Azimuth(
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
        degrees(ST_Azimuth(
            ST_PointN(edges_inbound.geom, -1)::geography,
            ST_PointN(edges_inbound.geom, -2)::geography
        )) AS azimuth
    FROM nodes AS n
    JOIN gis_core.centreline_latest AS edges_inbound
        ON n.node_id = edges_inbound.to_intersection_id
),

distances AS (
    SELECT
        legs.intersection_centreline_id,
        legs.leg_centreline_id,
        toronto_cardinal.leg_label,
        abs(180 - abs((toronto_cardinal.d - legs.azimuth)::numeric % 360 - 180)) AS angular_distance,
        stub_geom
    FROM legs
    CROSS JOIN toronto_cardinal
),

leg1 AS (
    SELECT DISTINCT ON (intersection_centreline_id)
        intersection_centreline_id,
        leg_centreline_id,
        leg_label,
        angular_distance,
        stub_geom
    FROM distances
    ORDER BY
        intersection_centreline_id,
        angular_distance ASC
),

leg2 AS (
    SELECT DISTINCT ON (intersection_centreline_id)
        intersection_centreline_id,
        leg_centreline_id,
        leg_label,
        angular_distance,
        stub_geom
    FROM distances
    WHERE NOT EXISTS (
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
    SELECT DISTINCT ON (intersection_centreline_id)
        intersection_centreline_id,
        leg_centreline_id,
        leg_label,
        angular_distance,
        stub_geom
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
    SELECT DISTINCT ON (intersection_centreline_id)
        intersection_centreline_id,
        leg_centreline_id,
        leg_label,
        angular_distance,
        stub_geom
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
    SELECT * FROM leg1
    UNION
    SELECT * FROM leg2
    UNION 
    SELECT * FROM leg3
    UNION
    SELECT * FROM leg4
)

SELECT DISTINCT ON (
    unified_legs.intersection_centreline_id,
    unified_legs.leg_centreline_id
)
    unified_legs.intersection_centreline_id,
    unified_legs.leg_centreline_id,
    unified_legs.leg_label AS leg,
    -- first node of the leg is the intersection
    ST_PointN(unified_legs.stub_geom, -1) AS intersection_geom,
    edges.linear_name_full_legal AS street_name,
    unified_legs.stub_geom AS leg_stub_geom,
    edges.geom AS leg_full_geom
FROM unified_legs
JOIN gis_core.centreline_latest AS edges
    ON unified_legs.leg_centreline_id = edges.centreline_id
ORDER BY
    unified_legs.intersection_centreline_id,
    unified_legs.leg_centreline_id,
    unified_legs.angular_distance ASC; -- lop off any repeated legs

/*
ALTER MATERIALIZED VIEW vz_intersection.lpi_centreline_leg_directions OWNER TO vz_intersection_admins;

CREATE UNIQUE INDEX ON vz_intersection.lpi_centreline_leg_directions (intersection_centreline_id, leg_centreline_id);
CREATE INDEX ON vz_intersection.lpi_centreline_leg_directions USING GIST (leg_full_geom);
CREATE INDEX ON vz_intersection.lpi_centreline_leg_directions USING GIST (leg_stub_geom);

CREATE INDEX ON vz_intersection.lpi_centreline_leg_directions (intersection_centreline_id);

COMMENT ON MATERIALIZED VIEW vz_intersection.lpi_centreline_leg_directions
IS 'Automated mapping of centreline intersection legs onto the four cardinal directions';

COMMENT ON COLUMN vz_intersection.lpi_centreline_leg_directions.leg_stub_geom
IS 'first (up to) 30m of the centreline segment geometry pointing inbound toward the intersection';

COMMENT ON COLUMN vz_intersection.lpi_centreline_leg_directions.leg
IS 'N, S, E, W cardinal direction';

COMMENT ON COLUMN vz_intersection.lpi_centreline_leg_directions.leg_full_geom
IS 'complete geometry of the centreline edge';
*/
