-- View: gis_core.centreline_routing_restrictions

-- DROP VIEW gis_core.centreline_routing_restrictions;

CREATE OR REPLACE VIEW gis_core.centreline_routing_restrictions
 AS
 SELECT DISTINCT ARRAY[s.id, e.id] AS path,
    9999 AS cost
   FROM gis_core.intersection_latest
     LEFT JOIN gis_core.routing_centreline_directional s ON s.centreline_id = intersection_latest.centreline_id_from
     LEFT JOIN gis_core.routing_centreline_directional e ON e.centreline_id = intersection_latest.centreline_id_to
  WHERE intersection_latest.connected = 'N'::text AND s.id IS NOT NULL AND e.id IS NOT NULL;

ALTER TABLE gis_core.centreline_routing_restrictions
    OWNER TO gis_admins;
COMMENT ON VIEW gis_core.centreline_routing_restrictions
    IS 'Centreline turning restriction table for routing uses, created with gis_core.intersection_latest, only includes sets of edges with restricted path. ';

GRANT SELECT ON TABLE gis_core.centreline_routing_restrictions TO bdit_humans;
GRANT ALL ON TABLE gis_core.centreline_routing_restrictions TO gis_admins;

