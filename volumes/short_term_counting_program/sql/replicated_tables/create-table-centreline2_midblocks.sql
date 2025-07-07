-- Table: traffic.centreline2_midblocks

-- DROP TABLE IF EXISTS traffic.centreline2_midblocks;

CREATE TABLE IF NOT EXISTS traffic.centreline2_midblocks
(
    midblock_id bigint NOT NULL,
    midblock_name text COLLATE pg_catalog."default" NOT NULL,
    centreline_type smallint NOT NULL,
    feature_code bigint NOT NULL,
    linear_name_id bigint NOT NULL,
    linear_name_full text COLLATE pg_catalog."default" NOT NULL,
    properties jsonb,
    from_intersection_id bigint,
    from_intersection_name text COLLATE pg_catalog."default",
    to_intersection_id bigint,
    to_intersection_name text COLLATE pg_catalog."default",
    lat double precision NOT NULL,
    lng double precision NOT NULL,
    geom geometry NOT NULL,
    centreline_id_array bigint [],
    shape_length numeric
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS traffic.centreline2_midblocks
OWNER TO traffic_bot;

REVOKE ALL ON TABLE traffic.centreline2_midblocks FROM bdit_humans;
REVOKE ALL ON TABLE traffic.centreline2_midblocks FROM dbadmin;

GRANT REFERENCES,
SELECT,
TRIGGER ON TABLE traffic.centreline2_midblocks TO bdit_humans WITH GRANT OPTION;

GRANT SELECT ON TABLE traffic.centreline2_midblocks TO dbadmin;

GRANT ALL ON TABLE traffic.centreline2_midblocks TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE traffic.centreline2_midblocks TO replicator_bot;

GRANT ALL ON TABLE traffic.centreline2_midblocks TO traffic_bot;

COMMENT ON TABLE traffic.centreline2_midblocks
IS 'Contains the MOVE centreline midblock network, these midblocks have been simplified from the gcc set to be more representative of a transportation users view of the network. `midblock_id` is the parent column and `centreline_id_array` contains the full list of GCC centrelines that were unioned into a single geom.
Copied from "move_staging"."centreline2_midblocks" by bigdata repliactor DAG at 2025-07-04 13:50.';