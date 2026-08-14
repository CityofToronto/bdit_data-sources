INSERT INTO miovision_api.alerts AS n (
    alert_id, start_time, end_time, intersection_id, alert
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (alert_id)
DO UPDATE
SET
    intersection_id = EXCLUDED.intersection_id,
    alert = EXCLUDED.alert,
    start_time = EXCLUDED.start_time,
    end_time = EXCLUDED.end_time
WHERE n.alert_id = EXCLUDED.alert_id;
