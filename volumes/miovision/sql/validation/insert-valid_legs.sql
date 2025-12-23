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
    AND valid2.intersection_uid IS NULL --anti join
ON CONFLICT ON CONSTRAINT valid_legs_start_date_class_pkey
DO NOTHING;

--update end dates
UPDATE miovision_validation.valid_legs AS vl
SET end_date = vlv.end_date
FROM miovision_validation.valid_legs_view AS vlv
WHERE
    vl.intersection_uid = vlv.intersection_uid
    AND vl.start_date = vlv.start_date
    AND vl.classification  = vlv.classification
    AND vl.leg = vlv.leg
    AND coalesce(vl.end_date, '1990-01-01') != coalesce(vlv.end_date, '1990-01-01');
