-- View: traffic.centreline2_traffic_signal

-- DROP VIEW traffic.centreline2_traffic_signal;

CREATE OR REPLACE VIEW traffic.centreline2_traffic_signal
AS
SELECT
    centreline2_traffic_signal.px,
    centreline2_traffic_signal.activation_date,
    centreline2_traffic_signal.lpi,
    centreline2_traffic_signal.node_id,
    CASE
        WHEN
            centreline2_traffic_signal.centreline_type = 1
            THEN centreline2_traffic_signal.centreline_id
        ELSE NULL::integer
    END AS midblock_id,
    CASE
        WHEN
            centreline2_traffic_signal.centreline_type = 2
            THEN centreline2_traffic_signal.centreline_id
        ELSE NULL::integer
    END AS intersection_id,
    centreline2_traffic_signal.lat,
    centreline2_traffic_signal.lng,
    centreline2_traffic_signal.signal_geom
FROM traffic_staging.centreline2_traffic_signal;

ALTER TABLE traffic.centreline2_traffic_signal
OWNER TO traffic_bot;
COMMENT ON VIEW traffic.centreline2_traffic_signal
IS 'Contains a mapping of `midblock_id`/`intersection_id` to `px` crossing numbers.';

GRANT SELECT,
REFERENCES,
TRIGGER ON TABLE traffic.centreline2_traffic_signal TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE traffic.centreline2_traffic_signal TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE traffic.centreline2_traffic_signal TO traffic_bot;
