--DROP VIEW miovision_api.open_issues;
CREATE OR REPLACE VIEW miovision_api.open_issues AS 

SELECT
    ar.uid,
    ar.intersection_uid,
    i.id AS intersection_id,
    i.intersection_name,
    ar.classification_uid,
    CASE
        WHEN ar.classification_uid = 2 THEN 'Bicycle TMC'
        WHEN ar.classification_uid = 10 THEN 'Bicycle Approach'
        WHEN ar.classification_uid IS NULL THEN 'All modes'
        ELSE c.classification
    END,
    ar.range_start::date,
    (current_timestamp AT TIME ZONE 'EST5EDT')::date - ar.range_start::date AS num_days,
    ar.notes
FROM miovision_api.anomalous_ranges AS ar
LEFT JOIN miovision_api.classifications AS c USING (classification_uid)
LEFT JOIN miovision_api.intersections AS i USING (intersection_uid)
WHERE
    intersection_uid IS NOT NULL --these will go under discontinuities
    AND (
        range_end IS NULL
        OR (notes LIKE '%identified by a daily airflow process%'
            AND range_end = (current_timestamp AT TIME ZONE 'EST5EDT')::date --today
        )
    )
ORDER BY
    intersection_uid,
    range_start,
    classification_uid;

COMMENT ON VIEW miovision_api.open_issues
IS '''A view to export open ended anomalous_ranges for communication with Miovision.
Converts intersection_uid and classification_uid into formats familiar to Miovision
(intersections.id, classifications.classification). anomalous_ranges.id col can be
used to link response back to table.''';
