WITH ars AS (
    SELECT
        uid,
        intersection_name || ' (' || intersection_uid || ')' AS intersection,
        COALESCE(
            classification || ' (' || classification_uid || ')',
            'All classifications'
        ) AS classification,
        leg,
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
        'uid: `' || ars.uid || '`, `'
        || ars.intersection || '`, `'
        || ars.classification || '`, `'
        || CASE WHEN ars.leg IS NOT NULL THEN ars.leg || ' leg`, ' ELSE '' END
        || 'volume: `' || ars.last_week_volume || '`, '
        || 'notes: `' || LEFT(ars.notes, 80)
        || CASE WHEN LENGTH(ars.notes) > 80 THEN '...' ELSE '' END || '`'
    ) AS gaps
FROM ars