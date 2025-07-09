-- Table: traffic.svc_metadata

-- DROP TABLE IF EXISTS traffic.svc_metadata;

CREATE TABLE IF NOT EXISTS traffic.svc_metadata
(
    study_id integer NOT NULL,
    study_start_date date NOT NULL,
    study_end_date date NOT NULL,
    study_type text COLLATE pg_catalog."default" NOT NULL,
    study_duration integer NOT NULL,
    study_location_name text COLLATE pg_catalog."default" NOT NULL,
    study_source text COLLATE pg_catalog."default" NOT NULL,
    study_geom geometry NOT NULL,
    midblock_id integer,
    centreline_type integer,
    centreline_feature_code integer,
    centreline_road_id integer,
    centreline_properties jsonb,
    CONSTRAINT metadata_pkey PRIMARY KEY (study_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS traffic.svc_metadata
OWNER TO traffic_bot;

REVOKE ALL ON TABLE traffic.svc_metadata FROM bdit_humans;

GRANT SELECT ON TABLE traffic.svc_metadata TO bdit_humans;

GRANT ALL ON TABLE traffic.svc_metadata TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE traffic.svc_metadata TO traffic_bot;

COMMENT ON TABLE traffic.svc_metadata
IS 'Table containing the metadata for SVCs.
Documentation: https://github.com/CityofToronto/bdit_data-sources/blob/master/volumes/short_term_counting_program/README.md#trafficsvc_metadata';
