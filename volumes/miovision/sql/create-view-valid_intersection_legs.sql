CREATE OR REPLACE VIEW miovision.valid_intersection_legs AS
SELECT 		intersection_uid, 
		leg, 
		dir, 
		CASE WHEN classification_uid IN (1,4,5) THEN 'Vehicles' WHEN classification_uid IN (2,7) THEN 'Bicycles' WHEN classification_uid IN (6) THEN 'Pedestrians' END AS class_group

FROM		miovision.volumes_15min
INNER JOIN	miovision.intersections USING (intersection_uid)
WHERE 		classification_uid NOT IN (3)
GROUP BY 	intersection_name, 
		intersection_uid, 
		leg, 
		dir, 
		CASE WHEN classification_uid IN (1,4,5) THEN 'Vehicles' WHEN classification_uid IN (2,7) THEN 'Bicycles' WHEN classification_uid IN (6) THEN 'Pedestrians' END
HAVING 		SUM(volume)/COUNT(DISTINCT datetime_bin) > 2.0 OR (CASE WHEN classification_uid IN (1,4,5) THEN 'Vehicles' WHEN classification_uid IN (2,7) THEN 'Bicycles' WHEN classification_uid IN (6) THEN 'Pedestrians' END) IN ('Bicycles', 'Pedestrians')
ORDER BY 	CASE WHEN classification_uid IN (1,4,5) THEN 'Vehicles' WHEN classification_uid IN (2,7) THEN 'Bicycles' WHEN classification_uid IN (6) THEN 'Pedestrians' END desc, intersection_uid, leg