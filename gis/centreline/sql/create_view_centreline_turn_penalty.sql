-- View: gis_core.centreline_turn_penalization

CREATE OR REPLACE VIEW gis_core.centreline_turn_penalization
AS
WITH full_restrictions AS (
    SELECT DISTINCT
        ARRAY[s.id, e.id] AS path,
        --- cost assignment in meters
        500 AS cost
    FROM gis_core.intersection_latest
    LEFT JOIN
        gis_core.routing_centreline_directional AS s
        ON intersection_latest.centreline_id_from = s.centreline_id
    LEFT JOIN
        gis_core.routing_centreline_directional AS e
        ON intersection_latest.centreline_id_to = e.centreline_id
    WHERE
        intersection_latest.linear_name_id_to != intersection_latest.linear_name_id_from
        AND s.id IS NOT NULL AND e.id IS NOT NULL
        --- avoid conflicts with the other restrictions table
        AND intersection_latest.connected = 'Y'
    --- include restrictions table for ease of use
    UNION ALL
    SELECT * FROM gis_core.centreline_routing_restrictions
)

--- to handle duplicates, we will select the max cost to prioritize full disconnection
SELECT DISTINCT
    path,
    MAX(cost) AS cost
FROM full_restrictions
GROUP BY path;

ALTER TABLE gis_core.centreline_turn_penalization
OWNER TO gis_admins;
COMMENT ON VIEW gis_core.centreline_turn_penalization
IS 'Centreline turning penalization table. This assigns a cost to non-through movements, and restricts non-connected movements';

GRANT SELECT ON TABLE gis_core.centreline_turn_penalization TO bdit_humans;
GRANT ALL ON TABLE gis_core.centreline_turn_penalization TO gis_admins;