-- Table: traffic_staging.centreline2_traffic_signal

-- DROP TABLE IF EXISTS traffic_staging.centreline2_traffic_signal;

CREATE TABLE IF NOT EXISTS traffic_staging.centreline2_traffic_signal
(
    px integer,
    activation_date date,
    lpi text COLLATE pg_catalog."default",
    node_id integer,
    centreline_id integer,
    centreline_type integer,
    lat double precision,
    lng double precision,
    signal_geom GEOMETRY (POINT, 4326)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS traffic_staging.centreline2_traffic_signal
OWNER TO traffic_bot;

REVOKE ALL ON TABLE traffic_staging.centreline2_traffic_signal FROM bdit_humans;

GRANT REFERENCES,
SELECT,
TRIGGER ON TABLE traffic_staging.centreline2_traffic_signal TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE traffic_staging.centreline2_traffic_signal TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE traffic_staging.centreline2_traffic_signal TO traffic_bot;

COMMENT ON TABLE traffic_staging.centreline2_traffic_signal
IS '(DO NOT USE) Used in the replication of traffic.centreline2_traffic_signal';
