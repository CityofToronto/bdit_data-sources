-- Table: traffic_staging.tmc_metadata

-- DROP TABLE IF EXISTS traffic_staging.tmc_metadata;

CREATE TABLE IF NOT EXISTS traffic_staging.tmc_metadata
(
    count_id bigint NOT NULL,
    count_date date NOT NULL,
    count_type text COLLATE pg_catalog."default" NOT NULL,
    count_duration text COLLATE pg_catalog."default" NOT NULL,
    count_location_name text COLLATE pg_catalog."default" NOT NULL,
    count_source text COLLATE pg_catalog."default" NOT NULL,
    centreline_id integer,
    centreline_type smallint,
    centreline_feature_code integer,
    centreline_intersection_classification character varying(5) COLLATE pg_catalog."default",
    centreline_properties jsonb,
    count_geom geometry NOT NULL,
    CONSTRAINT tmc_metadata_pkey PRIMARY KEY (count_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS traffic_staging.tmc_metadata
OWNER TO traffic_bot;

REVOKE ALL ON TABLE traffic_staging.tmc_metadata FROM bdit_humans;

GRANT SELECT ON TABLE traffic_staging.tmc_metadata TO bdit_humans;

GRANT ALL ON TABLE traffic_staging.tmc_metadata TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE traffic_staging.tmc_metadata TO traffic_bot;

COMMENT ON TABLE traffic_staging.tmc_metadata
IS '(DO NOT USE) Used in the replication of traffic.tmc_metadata';
-- Index: tmc_metadata_count_date_idx

-- DROP INDEX IF EXISTS traffic_staging.tmc_metadata_count_date_idx;

CREATE INDEX IF NOT EXISTS tmc_metadata_count_date_idx
ON traffic_staging.tmc_metadata USING btree
(count_date ASC NULLS LAST)
TABLESPACE pg_default;
