WITH ars AS (
    SELECT
        uid,
        intersection_name || ' (' || intersection_uid || ')' AS intersection,
        COALESCE(
            classification || ' (' || classification_uid || ')',
            'All classifications'
        ) AS classification,
        last_week_volume,
        notes
    FROM miovision_api.open_issues
    WHERE last_week_volume > 0
    ORDER BY uid
)

SELECT
    NOT(COUNT(*) > 0) AS _check,
    CASE WHEN COUNT(*) = 1 THEN 'There is ' ELSE 'There are ' END || COUNT(*)
        || ' open ended anomalous_range(s) with non-zero volumes in the last 7 days:'
    AS summ,
    array_agg(
        'ar.uid: `' || ars.uid || '`' || chr(10)
        || 'intersection: `' || ars.intersection || '`' || chr(10)
        || 'classification: `' || ars.classification || '`' || chr(10)
        || 'volume: `' || ars.last_week_volume || '`' || chr(10)
        || 'notes: `' || LEFT(ars.notes, 100)
        || CASE WHEN LENGTH(ars.notes) > 100 THEN '...' ELSE '' END || '`' || chr(10)
    ) AS gaps
FROM ars