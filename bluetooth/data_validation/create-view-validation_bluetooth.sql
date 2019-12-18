--Find out reference_id for bt sensor that was used in king_pilot
CREATE OR REPLACE VIEW jchew.validation_bluetooth AS
SELECT X.bt_id, X.analysis_id, Y."referenceId" AS reference_id, X.street_name, X.direction, 
X.from_intersection AS from_intersection_name, X.to_intersection AS to_intersection_name, 
Y."fromIntersectionId" AS from_intersection_id, Y."toIntersectionId" AS to_intersection_id, 
Y."roadClass" AS road_class, Y.direction AS dir, Y."referenceLength" AS reference_length, 
Y.side, Y.section, Y.score, Y."geometryId"
 FROM king_pilot.bt_segments X
LEFT JOIN natalie.all_ssbt Y
ON X.analysis_id = Y.analysis_id
ORDER BY bt_id

--OLD ONE (NOT USED)
CREATE MATERIALIZED VIEW jchew.validation_bluetooth AS
SELECT X.bt_id, X.analysis_id, Y.reference_id, X.street_name, X.direction, 
X.from_intersection AS from_intersection_name, X.to_intersection AS to_intersection_name, 
Y.from_intersection, Y.to_intersection, Y.road_class, Y.direction AS dir, Y.reference_length,
Y.side, Y.section, Y.score, Y.geometry
 FROM king_pilot.bt_segments X
LEFT JOIN gis_shared_streets.bluetooth_matched Y
ON X.analysis_id::text = Y.analysis_id
ORDER BY bt_id

