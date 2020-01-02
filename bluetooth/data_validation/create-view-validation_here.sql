--Match bt and here using reference_id (after re-routing is done, AGAIN)
CREATE OR REPLACE VIEW jchew.validation_bt_here AS
SELECT X.bt_id, X.analysis_id, X.reference_id, Y."shstReferenceId" , X.street_name, X.direction,
X.from_intersection_name, X.to_intersection_name, X.reference_length, 
Y.id, Y.pp_link_dir, Y.geom
 FROM jchew.validation_bluetooth_geom X
LEFT JOIN natalie.here_matched_180430 Y
ON X.reference_id = Y."shstReferenceId"
AND X.from_intersection_id = Y."shstFromIntersectionId"
AND X.to_intersection_id = Y."shstToIntersectionId"
ORDER BY id