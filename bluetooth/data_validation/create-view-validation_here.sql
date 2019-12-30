--De-json the table first to get `referenceId`
--courtesy of https://www.reddit.com/r/PostgreSQL/comments/2u6ah3/how_to_use_json_to_recordset_on_json_stored_in_a/co93w50/?context=8&depth=9
CREATE OR REPLACE VIEW jchew.here_routing_matched_snapped_dejson AS 
SELECT 
	s.ogc_fid,
	s.pp_link_dir,
	t."geometryId" AS geometry_id,
	t."referenceId" AS reference_id,
	t."referenceLength" AS reference_length,
	t."section"::json->>0 AS section_lower,
	t."section"::json->>1 AS section_upper,
	t."fromIntersectionId" AS from_intersection_id,
	t."toIntersectionId" AS to_intersection_id,
	s.score,
	s.matchtype,
	s.wkb_geometry
	
FROM
	gis_shared_streets.here_routing_matched_snapped s
	CROSS JOIN LATERAL
	json_to_recordset(s.segments::json) AS 
	t("geometryId" text, "referenceId" text, "referenceLength" text, 
	  "section" text, "fromIntersectionId" text, "toIntersectionId" text)
ORDER BY s.ogc_fid


--Using matched here and bt done for other project to get link_dir (NO KING ST)
CREATE OR REPLACE VIEW jchew.validation_bt_here_no_king AS
SELECT * FROM king_pilot.bt_segments
LEFT JOIN gis_shared_streets.ak_bt_here2
USING (analysis_id)
ORDER BY analysis_id


--Match bt and here using reference_id (MAYBE)
CREATE OR REPLACE VIEW jchew.validation_bt_here_maybe AS
SELECT X.bt_id, X.analysis_id, X.reference_id, X.street_name, X.direction,
X.from_intersection_name, X.to_intersection_name, X.reference_length, 
Y.ogc_fid, Y.pp_link_dir, Y.wkb_geometry
 FROM jchew.validation_bluetooth X
LEFT JOIN jchew.here_routing_matched_snapped_dejson Y
ON X.reference_id = Y.reference_id
--AND X.from_intersection_id = Y.from_intersection_id
--AND X.to_intersection_id = Y.to_intersection_id
ORDER BY ogc_fid

--Match bt and here using reference_id (after re-routing is done, AGAIN)
CREATE OR REPLACE VIEW jchew.validation_bt_here_again AS
SELECT X.bt_id, X.analysis_id, X.reference_id, Y."shstReferenceId" , X.street_name, X.direction,
X.from_intersection_name, X.to_intersection_name, X.reference_length, 
Y.id, Y.pp_link_dir, Y.geom
 FROM jchew.validation_bluetooth X
LEFT JOIN natalie.here_matched_180430 Y
ON X.reference_id = Y."shstReferenceId"
AND X.from_intersection_id = Y."shstFromIntersectionId"
AND X.to_intersection_id = Y."shstToIntersectionId"
ORDER BY id