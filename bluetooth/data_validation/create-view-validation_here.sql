--Using matched here and bt done for other project to get link_dir (NO KING ST)
CREATE OR REPLACE VIEW jchew.validation_bt_here_no_king AS
SELECT * FROM king_pilot.bt_segments
LEFT JOIN gis_shared_streets.ak_bt_here2
USING (analysis_id)
ORDER BY analysis_id


--Match bt and here using reference_id (after re-routing is done, AGAIN)
CREATE OR REPLACE VIEW jchew.validation_bt_here_again AS
SELECT X.bt_id, X.analysis_id, X.reference_id, Y."shstReferenceId" , X.street_name, X.direction,
X.from_intersection_name, X.to_intersection_name, X.reference_length, 
Y.id, Y.pp_link_dir, Y.geom
 FROM jchew.validation_bluetooth_geom X
LEFT JOIN natalie.here_matched_180430 Y
ON X.reference_id = Y."shstReferenceId"
AND X.from_intersection_id = Y."shstFromIntersectionId"
AND X.to_intersection_id = Y."shstToIntersectionId"
ORDER BY id