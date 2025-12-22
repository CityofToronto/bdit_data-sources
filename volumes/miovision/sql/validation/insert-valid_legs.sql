INSERT INTO miovision_validation.valid_legs (
    intersection_uid, intersection_name, leg, start_date,
    end_date, classification, classification_uids
)

SELECT
    valid1.intersection_uid,
    valid1.intersection_name,
    valid1.leg,
    valid1.start_date,
    valid1.end_date,
    valid1.classification,
    valid1.classification_uids
FROM miovision_validation.valid_legs_view AS valid1
LEFT JOIN miovision_validation.valid_legs AS valid2
    USING (intersection_uid, start_date, classification, leg)
WHERE
    valid1.all_pass
    AND (
        valid2.intersection_uid IS NULL --anti join
        OR coalesce(valid2.end_date, '1990-01-01') != coalesce(valid1.end_date, '1990-01-01')
    )
ON CONFLICT ON CONSTRAINT valid_legs_start_date_class_pkey
DO UPDATE
SET end_date = EXCLUDED.end_date;
