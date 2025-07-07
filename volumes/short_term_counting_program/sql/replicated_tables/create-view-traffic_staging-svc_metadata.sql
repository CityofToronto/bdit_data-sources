-- View: traffic_staging.svc_metadata

-- DROP VIEW traffic_staging.svc_metadata;

CREATE OR REPLACE VIEW traffic_staging.svc_metadata
AS
SELECT
    svc_metadata.study_id,
    svc_metadata.study_start_date,
    svc_metadata.study_end_date,
    svc_metadata.study_type,
    svc_metadata.study_duration,
    svc_metadata.study_location_name,
    svc_metadata.study_source,
    svc_metadata.study_geom,
    svc_metadata.midblock_id AS centreline_id,
    svc_metadata.centreline_feature_code,
    svc_metadata.centreline_road_id,
    svc_metadata.centreline_properties
FROM traffic.svc_metadata;

ALTER TABLE traffic_staging.svc_metadata
OWNER TO traffic_bot;
COMMENT ON VIEW traffic_staging.svc_metadata
IS '(DO NOT USE) Used in the replication of traffic.svc_metadata';

GRANT SELECT,
REFERENCES,
TRIGGER ON TABLE traffic_staging.svc_metadata TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE traffic_staging.svc_metadata TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE traffic_staging.svc_metadata TO traffic_bot;
