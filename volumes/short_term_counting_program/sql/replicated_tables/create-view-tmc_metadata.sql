-- View: traffic.tmc_metadata

-- DROP VIEW traffic.tmc_metadata;

CREATE OR REPLACE VIEW traffic.tmc_metadata
AS
SELECT
    tmc_metadata.count_id,
    tmc_metadata.count_date,
    tmc_metadata.count_type,
    tmc_metadata.count_duration,
    tmc_metadata.count_location_name,
    tmc_metadata.count_source,
    CASE
        WHEN tmc_metadata.centreline_type = 1 THEN tmc_metadata.centreline_id
        ELSE NULL::integer
    END AS midblock_id,
    CASE
        WHEN tmc_metadata.centreline_type = 2 THEN tmc_metadata.centreline_id
        ELSE NULL::integer
    END AS intersection_id,
    tmc_metadata.centreline_feature_code,
    tmc_metadata.centreline_intersection_classification,
    tmc_metadata.centreline_properties,
    tmc_metadata.count_geom
FROM traffic_staging.tmc_metadata;

ALTER TABLE traffic.tmc_metadata
OWNER TO traffic_bot;

COMMENT ON VIEW traffic.tmc_metadata
IS 'Count-level study metadata for TMCs that contains all counts including both 14 and 8 hour legacy counts. Studies have been joined to the MOVE midblock intersections.
Documentation: https://github.com/CityofToronto/bdit_data-sources/blob/master/volumes/short_term_counting_program/README.md#tmcmetadata';

GRANT SELECT, REFERENCES, TRIGGER ON TABLE traffic.tmc_metadata TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE traffic.tmc_metadata TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE traffic.tmc_metadata TO traffic_bot;
