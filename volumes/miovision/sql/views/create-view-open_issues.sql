--DROP VIEW miovision_api.open_issues;
CREATE OR REPLACE VIEW miovision_api.open_issues AS

--DROP VIEW miovision_api.open_issues;

WITH alerts AS (
    SELECT
        ar.uid,
        string_agg(DISTINCT alerts.alert, '; '::text) AS alerts
    FROM miovision_api.anomalous_ranges AS ar
    JOIN miovision_api.alerts
        ON alerts.intersection_uid = ar.intersection_uid
        AND tsrange(alerts.start_time, alerts.end_time)
        && tsrange(ar.range_start, ar.range_end)
    GROUP BY ar.uid
)

SELECT
    ar.uid,
    ar.intersection_uid,
    i.id AS intersection_id,
    i.api_name AS intersection_name,
    ar.classification_uid,
    CASE
        WHEN ar.classification_uid = 2 THEN 'Bicycle TMC'
        WHEN ar.classification_uid = 10 THEN 'Bicycle Approach'
        WHEN ar.classification_uid IS NULL THEN 'All modes'
        ELSE c.classification
    END,
    ar.leg,
    ar.range_start::date,
    (current_timestamp AT TIME ZONE 'EST5EDT')::date - ar.range_start::date AS num_days,
    ar.notes,
    SUM(v.volume) AS volume,
    alerts.alerts
FROM miovision_api.anomalous_ranges AS ar
--keep rows with null classification_uid
LEFT JOIN miovision_api.classifications AS c USING (classification_uid)
--omit null intersection_uids. These will go under discontinuities. 
JOIN miovision_api.intersections AS i USING (intersection_uid)
--find last week volume
LEFT JOIN miovision_api.volumes AS v
    ON ar.intersection_uid = v.intersection_uid
    --volume within the last 7 days and after AR started
    AND v.datetime_bin >= ar.range_start
    --prune the partitions
    AND v.datetime_bin >= current_date - interval '7 days'
    AND (
        ar.classification_uid = v.classification_uid
        OR ar.classification_uid IS NULL
    )
    AND (
        ar.leg = v.leg
        OR ar.leg IS NULL
    )
LEFT JOIN alerts ON alerts.uid = ar.uid
WHERE
    ar.problem_level <> 'valid-caveat'
    --currently active
    AND (
        ar.range_end IS NULL
        OR (
            ar.notes LIKE '%identified by a daily airflow process%'
            AND ar.range_end = (current_timestamp AT TIME ZONE 'EST5EDT')::date --today
        )
    )
GROUP BY
    ar.uid,
    ar.intersection_uid,
    i.id,
    i.api_name,
    ar.classification_uid,
    c.classification,
    ar.range_start,
    ar.notes,
    alerts.alerts
ORDER BY
    ar.intersection_uid,
    ar.range_start,
    ar.classification_uid;

COMMENT ON VIEW miovision_api.open_issues
IS '''A view to export open ended anomalous_ranges for communication with Miovision.
Converts intersection_uid and classification_uid into formats familiar to Miovision
(intersections.id, classifications.classification). anomalous_ranges.id col can be
used to link response back to table.''';

COMMENT ON COLUMN miovision_api.open_issues.volume
IS 'Volume recorded within the last 7 days or since anomalous_range began, whichever is later.'

ALTER TABLE miovision_api.open_issues OWNER TO miovision_admins;
GRANT SELECT ON TABLE miovision_api.open_issues TO bdit_humans;
