--Match bt and here using reference_id (after re-routing is done, AGAIN)
CREATE OR REPLACE VIEW jchew.validation_bt_here AS
SELECT
    x.bt_id,
    x.analysis_id,
    x.reference_id,
    y."shstReferenceId",
    x.street_name,
    x.direction,
    x.from_intersection_name,
    x.to_intersection_name,
    x.reference_length,
    y.id,
    y.pp_link_dir,
    y.geom
FROM jchew.validation_bluetooth_geom AS x
LEFT JOIN natalie.here_matched_180430 AS y
    ON
        x.reference_id = y."shstReferenceId"
        AND x.from_intersection_id = y."shstFromIntersectionId"
        AND x.to_intersection_id = y."shstToIntersectionId"
ORDER BY id