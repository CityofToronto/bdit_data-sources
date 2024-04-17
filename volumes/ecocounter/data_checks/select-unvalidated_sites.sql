WITH unvalidated_sites AS (
    SELECT
        s.site_description || ' (site_id: ' || s.site_id
        || ') - volume last {{ params.lookback }}: '
        || SUM(counts.volume) AS descrip -- noqa: TMP
    FROM ecocounter.sites AS s
    LEFT JOIN ecocounter.flows USING (site_id)
    LEFT JOIN ecocounter.counts_unfiltered AS counts
        ON counts.datetime_bin >= '{{ ds }} 00:00:00'::timestamp -- noqa: TMP
        + interval '1 day' - interval '{{ params.lookback }}' -- noqa: TMP
        AND flows.flow_id = counts.flow_id
    WHERE NOT (sites.validated)
    GROUP BY
        s.site_id,
        s.site_description
    HAVING SUM(counts.volume) > 0
    ORDER BY s.site_id
)

SELECT
    COUNT(*) < 1 AS check,
    CASE WHEN COUNT(*) = 1 THEN 'There is ' ELSE 'There are ' END ||
        COALESCE(COUNT(*), 0) ||
        CASE WHEN COUNT(*) = 1 THEN ' unvalidated site.' ELSE ' unvalidated sites.'
    END AS summ,
    array_agg(descrip) AS gaps
FROM unvalidated_sites