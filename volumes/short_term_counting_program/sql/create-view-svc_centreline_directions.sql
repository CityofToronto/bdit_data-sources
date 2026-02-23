/*
A mid-block SVC/ATR count is assigned two of the four cardinal directions.
These will be opposites (N/S or E/W), but a given centreline segment isn't
necessarily defined as N/S or E/W per se. A diagonal segment could be
assigned either pair while still being clear about which direction on the
segment is meant.

This view assigns directons, relative to the centreline segment, to each
of the four cardinal directions. Some of these, like N/S on Bloor are
extremely unlikely to show up in the data, and are excluded, but many other
unlikely combinations are kept just in case.

As such, this view should be joined on the actual directions assigned to SVCs
rather than used on its own. Such a join *should* filter out most silly values.
*/

CREATE MATERIALIZED VIEW traffic.svc_centreline_directions AS

WITH to_cardinal (bearing, direction) AS (
    -- define cardinal directions in degrees, but rotated
    -- by -17 degrees to match the orientation of the street grid
    VALUES
    (radians(360 - 17), 'NB'),
    (radians(90 - 17), 'EB'),
    (radians(180 - 17), 'SB'),
    (radians(270 - 17), 'WB')
)

SELECT
    cl.centreline_id,
    CASE
        WHEN r.reversed THEN cl.to_intersection_id
        ELSE cl.from_intersection_id
    END AS from_node,
    CASE
        WHEN r.reversed THEN cl.from_intersection_id
        ELSE cl.to_intersection_id
    END AS to_node,
    ad.direction,
    CASE
        WHEN r.reversed THEN ST_Reverse(cl.geom)
        ELSE cl.geom
    END AS centreline_geom_directed,
    CASE
        WHEN r.reversed THEN (180 - degrees(ad.angular_distance))::real
        ELSE degrees(ad.angular_distance)::real
    END AS absolute_angular_distance
FROM gis_core.centreline_latest AS cl
CROSS JOIN
    LATERAL (
        SELECT
            to_cardinal.direction,
            -- get the minimum angular distance between the compass bearing and centreline's azimuth
            LEAST( -- math is done in radians
                ABS(
                    ST_Azimuth(ST_PointN(cl.geom, 1)::geography, ST_PointN(cl.geom, -1)::geography)
                    - to_cardinal.bearing
                ),
                (2 * PI()) - ABS(
                    ST_Azimuth(ST_PointN(cl.geom, 1)::geography, ST_PointN(cl.geom, -1)::geography)
                    - to_cardinal.bearing
                )
            ) AS angular_distance
        FROM to_cardinal
    ) AS ad, -- ad for angular distance
    LATERAL (
        -- edge geometry should be reversed where such a reversal
        -- would reduce the angular distance
        SELECT ad.angular_distance > radians(90) AS reversed
    ) AS r
-- exclude results where the cardinal direction is orthogonal, +/- 10 degrees
WHERE
    ad.angular_distance < radians(80)
    OR ad.angular_distance > radians(100);

CREATE UNIQUE INDEX ON traffic.svc_centreline_directions (centreline_id, direction);

ALTER MATERIALIZED VIEW traffic.svc_centreline_directions OWNER TO gis_admins;

COMMENT ON MATERIALIZED VIEW traffic.svc_centreline_directions
IS 'Maps the four cardinal directions (NB, SB, EB, & WB) referenced by SVCs onto '
'specific directions of travel along edges of the `gis_core.centreline_latest` network. '
'Refreshed automatically by `gcc_layers_pull_bigdata` DAG after inserts into '
'`gis_core.centreline_latest`.';

COMMENT ON COLUMN traffic.svc_centreline_directions.geom_directed
IS 'centreline segment geom drawn in the direction of `direction`';

COMMENT ON COLUMN traffic.svc_centreline_directions.absolute_angular_distance
IS 'Minimum absolute angular distance in degrees between centreline as drawn in `centreline_geom_directed` and the ideal of the stated direction. To be used as a measure of confidence for the match.';
