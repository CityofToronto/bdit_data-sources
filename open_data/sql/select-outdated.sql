WITH outdated AS (
    SELECT
        '<https://open.toronto.ca/dataset/' || page_id || '|`' || page_id || '`> is out of date. Last refreshed: `'
        || last_refreshed || '` (days old: ' || CURRENT_DATE - last_refreshed::date || ').' AS msg
    FROM open_data.od_pages
    WHERE last_refreshed < CURRENT_TIMESTAMP -
        CASE refresh_rate
            WHEN 'Daily' THEN interval '3 days'
            WHEN 'Monthly' THEN interval '60 days'
            WHEN 'Quarterly' THEN interval '120 days'
            ELSE interval '60 days'
        END
    ORDER BY page_id ASC
)

SELECT COUNT(*) = 0 AS _check, 'One or more Open Data pages is outdated:' AS msg, array_agg(outdated.msg)
FROM outdated;