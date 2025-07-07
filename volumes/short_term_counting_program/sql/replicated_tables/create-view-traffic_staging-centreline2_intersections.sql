-- View: traffic_staging.centreline2_intersections

-- DROP VIEW traffic_staging.centreline2_intersections;

CREATE OR REPLACE VIEW traffic_staging.centreline2_intersections
AS
SELECT
    centreline2_intersections.intersection_id AS centreline_id,
    centreline2_intersections.centreline_type,
    centreline2_intersections.intersection_name,
    centreline2_intersections.classification,
    centreline2_intersections.feature_code,
    centreline2_intersections.properties,
    centreline2_intersections.lat,
    centreline2_intersections.lng,
    centreline2_intersections.geom,
    centreline2_intersections.date_effective
FROM traffic.centreline2_intersections;

ALTER TABLE traffic_staging.centreline2_intersections
OWNER TO traffic_bot;
COMMENT ON VIEW traffic_staging.centreline2_intersections
IS '(DO NOT USE) Used in the replication of traffic.centreline2_intersections';

GRANT SELECT,
REFERENCES,
TRIGGER ON TABLE traffic_staging.centreline2_intersections TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE traffic_staging.centreline2_intersections TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE traffic_staging.centreline2_intersections TO traffic_bot;
