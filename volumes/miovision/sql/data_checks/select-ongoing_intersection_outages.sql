WITH all_outages AS (
    SELECT COUNT(*) AS cnt
    FROM miovision_api.open_issues_review
    WHERE COALESCE(classification_uid, 0) != 2
),

total_outages AS (
    SELECT
        intersection_name || ' (id: `' || intersection_id || '`) - data last received: `'
        || range_start || '` (' || CURRENT_DATE - range_start || ' days)' AS descrip
    FROM miovision_api.open_issues_review
    WHERE classification_uid IS NULL
)

SELECT
    (SELECT cnt FROM all_outages) < 1 AS _check,
    CASE WHEN COUNT(total_outages.*) = 1 THEN 'There is ' ELSE 'There are ' END
    || COALESCE(COUNT(total_outages.*), 0)
    || CASE WHEN COUNT(total_outages.*) = 1 THEN ' full outages.' ELSE ' full outages '
    || 'and ' || (SELECT cnt FROM all_outages) || ' partial outages. See `miovision_api.open_issues_review`.'
    END AS summ, --gap_threshold
    array_agg(total_outages.descrip || chr(10)) AS gaps
FROM total_outages