-- View: traffic_staging.centreline2_midblocks

-- DROP VIEW traffic_staging.centreline2_midblocks;

CREATE OR REPLACE VIEW traffic_staging.centreline2_midblocks
AS
SELECT
    centreline2_midblocks.midblock_id AS centreline_id,
    centreline2_midblocks.midblock_name,
    centreline2_midblocks.centreline_type,
    centreline2_midblocks.feature_code,
    centreline2_midblocks.linear_name_id,
    centreline2_midblocks.linear_name_full,
    centreline2_midblocks.properties,
    centreline2_midblocks.from_intersection_id,
    centreline2_midblocks.from_intersection_name,
    centreline2_midblocks.to_intersection_id,
    centreline2_midblocks.to_intersection_name,
    centreline2_midblocks.lat,
    centreline2_midblocks.lng,
    centreline2_midblocks.geom,
    centreline2_midblocks.centreline_id_array,
    centreline2_midblocks.shape_length
FROM traffic.centreline2_midblocks;

ALTER TABLE traffic_staging.centreline2_midblocks
OWNER TO traffic_bot;
COMMENT ON VIEW traffic_staging.centreline2_midblocks
IS '(DO NOT USE) Used in the replication of traffic.centreline2_midblocks';

GRANT ALL ON TABLE traffic_staging.centreline2_midblocks TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE traffic_staging.centreline2_midblocks TO replicator_bot;
GRANT ALL ON TABLE traffic_staging.centreline2_midblocks TO traffic_bot;
