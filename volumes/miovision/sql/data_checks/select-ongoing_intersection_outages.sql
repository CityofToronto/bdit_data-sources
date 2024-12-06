WITH all_outages AS (
    SELECT COUNT(*) AS cnt
    FROM miovision_api.open_issues
    WHERE COALESCE(classification_uid, 0) != 2
),

total_outages AS (
    SELECT
        i.intersection_name || ' (id: `' || i.id || '`) - data last received: `'
        || oi.range_start || '` (' || CURRENT_DATE - oi.range_start || ' days)' AS descrip
    FROM miovision_api.open_issues AS oi
    JOIN miovision_api.intersections AS i USING (intersection_uid)
    WHERE oi.classification_uid IS NULL
)

SELECT
    (SELECT cnt FROM all_outages) < 1 AS _check,
    CASE WHEN COUNT(total_outages.*) = 1 THEN 'There is ' ELSE 'There are ' END
    || COALESCE(COUNT(total_outages.*), 0)
    || CASE
        WHEN
            COUNT(total_outages.*) = 1 THEN ' full outages.' ELSE ' full outages '
        || 'and '
        || (SELECT cnt FROM all_outages
        )
        || ' partial outages. See `miovision_api.open_issues`.'
    END AS summ, --gap_threshold
    array_agg(total_outages.descrip) AS gaps
FROM total_outages
