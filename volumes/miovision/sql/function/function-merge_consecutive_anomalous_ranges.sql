CREATE OR REPLACE FUNCTION miovision_api.merge_consecutive_anomalous_ranges()
RETURNS void AS $$

--identify back to back ranges:
WITH consecutive_ranges AS (
    SELECT
        ar1.uid,
        ar2.uid AS ar_uid_to_delete,
        ar1.classification_uid,
        ar1.notes,
        ar1.investigation_level,
        ar1.problem_level,
        ar1.leg,
        ar1.range_start,
        ar2.range_end AS new_range_end
    FROM miovision_api.anomalous_ranges AS ar1
    JOIN miovision_api.anomalous_ranges AS ar2
    ON
        ar1.intersection_uid = ar2.intersection_uid
        AND COALESCE(ar1.classification_uid, 0) = COALESCE(ar2.classification_uid, 0)
        AND COALESCE(ar1.leg, '') = COALESCE(ar2.leg, '')
        AND ar1.notes = ar2.notes
        AND ar1.investigation_level = ar2.investigation_level
        AND ar1.problem_level = ar2.problem_level
        AND ar1.range_end = ar2.range_start
),

--delete the second one
deleted AS (
    DELETE FROM miovision_api.anomalous_ranges AS ar
    USING consecutive_ranges AS cr
    WHERE ar.uid = cr.ar_uid_to_delete
)

--update the first one
UPDATE miovision_api.anomalous_ranges AS ar
SET range_end = cr.new_range_end
FROM consecutive_ranges AS cr
WHERE ar.uid = cr.uid;

$$ LANGUAGE sql;

ALTER FUNCTION miovision_api.merge_consecutive_anomalous_ranges()
OWNER TO miovision_admins;

GRANT EXECUTE ON FUNCTION miovision_api.merge_consecutive_anomalous_ranges() TO miovision_api_bot;
