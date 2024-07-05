INSERT INTO miovision_api.alerts_new AS n (
    alert_id, start_time, end_time, intersection_id, alert
)
VALUES %s
ON CONFLICT (alert_id)
DO UPDATE
SET
    intersection_id = EXCLUDED.intersection_id,
    alert = EXCLUDED.alert,
    start_time = EXCLUDED.start_time,
    end_time = EXCLUDED.end_time
WHERE n.alert_id = EXCLUDED.alert_id;

--update foreign key referencing miovision_api.intersections
--handles new records as well as old records with null intersection_uid (newly added intersections)
UPDATE miovision_api.alerts_new AS n
SET intersection_uid = i.intersection_uid
FROM miovision_api.intersections AS i
WHERE
    n.intersection_id = i.id
    AND n.intersection_uid IS NULL;