--update foreign key referencing miovision_api.intersections
--handles new records as well as old records with null intersection_uid (newly added intersections)
UPDATE miovision_api.alerts AS n
SET intersection_uid = i.intersection_uid
FROM miovision_api.intersections AS i
WHERE
    n.intersection_id = i.id
    AND n.intersection_uid IS NULL;