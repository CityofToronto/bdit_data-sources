--Find out reference_id for bt sensor that was used in king_pilot (lookup table is more right)
CREATE OR REPLACE VIEW jchew.validation_bluetooth_geom AS
SELECT X.bt_id, X.analysis_id, Y."referenceId" AS reference_id, X.street_name, X.direction, 
X.from_intersection AS from_intersection_name, X.to_intersection AS to_intersection_name, 
Y."fromIntersectionId" AS from_intersection_id, Y."toIntersectionId" AS to_intersection_id, 
Y."roadClass" AS road_class, Y.direction AS dir, Y."referenceLength" AS reference_length, 
Y.side, Y.section, Y.score, Y."geometryId"
 FROM king_pilot.bt_segments X
LEFT JOIN natalie.all_ssbt_geom_distinct Y
ON X.analysis_id = Y.analysis_id
ORDER BY bt_id

--Check lookup table with query below by comparing the total length of an analysis_id
SELECT X.analysis_id, sum(X."referenceLength") AS lookup_length, Y.length AS bt_length
FROM natalie.all_ssbt_geom_distinct X  --OR USING natalie.all_ssbt
JOIN bluetooth.segments Y
USING (analysis_id)
WHERE analysis_id IN (1453262, 1453284, 1453305, 1453367, 1453395, 1453445, 
					  1453464, 1453483, 1454196, 1454209, 1454352, 1454366, 
					  1454523, 1454549, 1454670, 1454683, 1455243, 1455256, 1455385, 1455400)
GROUP BY X.analysis_id, Y.length
ORDER BY X.analysis_id

