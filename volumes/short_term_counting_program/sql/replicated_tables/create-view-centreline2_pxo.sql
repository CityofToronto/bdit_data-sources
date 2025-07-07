-- View: traffic.centreline2_pxo

-- DROP VIEW traffic.centreline2_pxo;

CREATE OR REPLACE VIEW traffic.centreline2_pxo
AS
SELECT
    centreline2_pxo.px,
    centreline2_pxo.px_name,
    CASE
        WHEN centreline2_pxo.centreline_type = 1 THEN centreline2_pxo.centreline_id
        ELSE NULL::integer
    END AS midblock_id,
    CASE
        WHEN centreline2_pxo.centreline_type = 2 THEN centreline2_pxo.centreline_id
        ELSE NULL::integer
    END AS intersection_id,
    centreline2_pxo.lat,
    centreline2_pxo.lng,
    centreline2_pxo.geom
FROM traffic_staging.centreline2_pxo;

ALTER TABLE traffic.centreline2_pxo
OWNER TO traffic_bot;
COMMENT ON VIEW traffic.centreline2_pxo
IS 'Contains a mapping of `midblock_id`/`intersection_id` to `pxo` crossing numbers.';

GRANT SELECT, REFERENCES, TRIGGER ON TABLE traffic.centreline2_pxo TO bdit_humans WITH GRANT OPTION;
GRANT ALL ON TABLE traffic.centreline2_pxo TO rds_superuser WITH GRANT OPTION;
GRANT ALL ON TABLE traffic.centreline2_pxo TO traffic_bot;
