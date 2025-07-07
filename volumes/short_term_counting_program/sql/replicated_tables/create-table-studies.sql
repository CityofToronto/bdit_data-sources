-- Table: traffic.studies

-- DROP TABLE IF EXISTS traffic.studies;

CREATE TABLE IF NOT EXISTS traffic.studies
(
    legacy boolean,
    "countLocationId" bigint,
    "studyType" character varying COLLATE pg_catalog."default",
    "countGroupId" bigint,
    "startDate" timestamp without time zone,
    "endDate" timestamp without time zone,
    duration bigint,
    "daysOfWeek" double precision [],
    hours text COLLATE pg_catalog."default",
    "centrelineType" integer,
    "centrelineId" bigint,
    geom geometry
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS traffic.studies
OWNER TO traffic_bot;

REVOKE ALL ON TABLE traffic.studies FROM bdit_humans;

GRANT SELECT ON TABLE traffic.studies TO bdit_humans;

GRANT ALL ON TABLE traffic.studies TO dbadmin;

GRANT ALL ON TABLE traffic.studies TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE traffic.studies TO traffic_bot;

COMMENT ON TABLE traffic.studies
IS 'Contains metadata for all study types available in MOVE.
Copied from "move_staging"."counts2_studies" by bigdata repliactor DAG at 2025-07-04 13:50.';
