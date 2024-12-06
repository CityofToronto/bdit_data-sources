WITH camera_details (
    id, camera_id, camera_label
) AS (
    VALUES %s --noqa: PRS
)

INSERT INTO miovision_api.camera_details (
    intersection_id, camera_id, camera_label, last_seen
)
SELECT
    cd.id,
    cd.camera_id,
    cd.camera_label,
    CURRENT_DATE AS last_seen
FROM camera_details AS cd
LEFT JOIN miovision_api.intersections USING (id)
ON CONFLICT (intersection_id, camera_id)
DO UPDATE SET
camera_label = excluded.camera_label,
last_seen = excluded.last_seen;