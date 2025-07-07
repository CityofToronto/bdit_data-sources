-- Table: traffic_staging.centreline2_pxo

-- DROP TABLE IF EXISTS traffic_staging.centreline2_pxo;

CREATE TABLE IF NOT EXISTS traffic_staging.centreline2_pxo
(
    px integer,
    px_name text COLLATE pg_catalog."default",
    centreline_id integer,
    centreline_type integer,
    lat double precision,
    lng double precision,
    geom GEOMETRY (POINT, 4326)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS traffic_staging.centreline2_pxo
OWNER TO traffic_bot;

REVOKE ALL ON TABLE traffic_staging.centreline2_pxo FROM bdit_humans;

GRANT REFERENCES,
SELECT,
TRIGGER ON TABLE traffic_staging.centreline2_pxo TO bdit_humans WITH GRANT OPTION;

GRANT ALL ON TABLE traffic_staging.centreline2_pxo TO rds_superuser WITH GRANT OPTION;

GRANT ALL ON TABLE traffic_staging.centreline2_pxo TO traffic_bot;

COMMENT ON TABLE traffic_staging.centreline2_pxo
IS '(DO NOT USE) Used in the replication of traffic.centreline2_pxo';
