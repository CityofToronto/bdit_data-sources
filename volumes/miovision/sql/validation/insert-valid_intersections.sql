--
INSERT INTO miovision_validation.valid_intersections (
    intersection_uid, intersection_name, start_date,
    end_date, classification, classification_uids
)

SELECT
    valid1.intersection_uid,
    valid1.intersection_name,
    valid1.start_date,
    valid1.end_date,
    valid1.classification,
    valid1.classification_uids
FROM miovision_validation.valid_intersections_view AS valid1
LEFT JOIN miovision_validation.valid_intersections AS valid2
    USING (intersection_uid, start_date, classification)
WHERE
    valid1.all_pass
    AND valid2.intersection_uid IS NULL --anti join existing
ON CONFLICT ON CONSTRAINT valid_intersections_start_date_class_pkey
DO NOTHING;

--update end dates
UPDATE miovision_validation.valid_intersections AS vi
SET end_date = viv.end_date
FROM miovision_validation.valid_intersections_view AS viv
WHERE
    vi.intersection_uid = viv.intersection_uid
    AND vi.start_date = viv.start_date
    AND vi.classification = viv.classification
    AND coalesce(vi.end_date, '1990-01-01') != coalesce(viv.end_date, '1990-01-01');
