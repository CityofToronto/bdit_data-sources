WITH ars AS (
    SELECT
        uid,
        intersection_name || ' (' || intersection_uid || ')' AS intersection,
        COALESCE(
            classification || ' (' || classification_uid || ')',
            'All classifications'
        ) AS classification,
        leg,
        volume,
        notes
    FROM miovision_api.open_issues_review
    WHERE volume > 0
    ORDER BY uid
)

SELECT
    NOT(COUNT(*) > 0) AS _check,
    CASE WHEN COUNT(*) = 1 THEN 'There is ' ELSE 'There are ' END || COUNT(*)
    || ' open ended anomalous_range(s) with non-zero volumes within the last 7 days (or since AR began):'
    AS summ,
    array_agg(
        'uid: `' || ars.uid || '`, `'
        || ars.intersection || '`, `'
        || ars.classification || '`, '
        || CASE WHEN ars.leg IS NOT NULL THEN '`' || ars.leg || ' leg`, ' ELSE '' END
        || 'volume: `' || to_char(ars.volume, 'FM9,999,999') || '`, '
        || 'notes: `' || LEFT(ars.notes, 80)
        || CASE WHEN LENGTH(ars.notes) > 80 THEN '...' ELSE '' END || '`'
    ) AS gaps
FROM ars