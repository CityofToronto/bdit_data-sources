-- Table: traffic.centreline2_intersections

-- DROP TABLE IF EXISTS traffic.centreline2_intersections;

CREATE TABLE IF NOT EXISTS traffic.centreline2_intersections
(
    intersection_id bigint NOT NULL,
    centreline_type smallint NOT NULL,
    intersection_name text COLLATE pg_catalog."default",
    classification text COLLATE pg_catalog."default" NOT NULL,
    feature_code integer NOT NULL,
    properties jsonb,
    lat double precision NOT NULL,
    lng double precision NOT NULL,
    geom geometry NOT NULL,
    date_effective timestamp without time zone NOT NULL
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS traffic.centreline2_intersections
OWNER TO traffic_bot;

REVOKE ALL ON TABLE traffic.centreline2_intersections FROM bdit_humans;

GRANT REFERENCES,
SELECT,
TRIGGER ON TABLE traffic.centreline2_intersections TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE traffic.centreline2_intersections TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE traffic.centreline2_intersections TO replicator_bot;

GRANT ALL ON TABLE traffic.centreline2_intersections TO traffic_bot;

COMMENT ON TABLE traffic.centreline2_intersections
IS 'Contains the MOVE centreline intersection network. These intersections have been filtered to only include intersections that are used in the MOVE midblock network (traffic.centreline2_midblocks).';
