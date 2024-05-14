WITH unvalidated_sites AS (
    SELECT
        site.site_id,
        site.site_description,
        flow.flow_id,
        site.site_description || ' (site_id: ' || site.site_id
        || (CASE WHEN flow.flow_id IS NOT NULL THEN ', channel_id: ' || flow.flow_id ELSE '' END)
        || ') - volume last {{ params.lookback }}: ' -- noqa: TMP
        || COALESCE(SUM(counts.volume), 0) AS descrip
    FROM ecocounter.sites_unfiltered AS site
    LEFT JOIN ecocounter.flows_unfiltered AS flow USING (site_id)
    LEFT JOIN ecocounter.counts_unfiltered AS counts
        ON counts.datetime_bin >= '{{ data_interval_end }} 00:00:00'::timestamp -- noqa: TMP
        + interval '1 day' - interval '{{ params.lookback }}' -- noqa: TMP
        AND flow.flow_id = counts.flow_id
    WHERE
        --validated = False is largely ignored
        site.validated IS NULL
        OR flow.validated IS NULL
    GROUP BY
        --find both sites and site/flow combos which are out of order.
        GROUPING SETS ((site.site_id), (site.site_id, flow.flow_id)),
        site.site_description
    HAVING SUM(counts.volume) > 0
    ORDER BY site.site_id, flow.flow_id
),

dedup AS (
    --out of order sites (all channels)
    SELECT descrip
    FROM unvalidated_sites
    WHERE flow_id IS NULL

    UNION

    --out of order flows (where channel does not have overall outage)
    SELECT ooo_flows.descrip
    FROM unvalidated_sites AS ooo_flows
    --anti join ooo_sites
    LEFT JOIN unvalidated_sites AS ooo_sites
        ON ooo_flows.site_id = ooo_sites.site_id
        AND ooo_sites.flow_id IS NULL
    WHERE
        ooo_flows.flow_id IS NOT NULL
        AND ooo_sites.site_id IS NULL --anti join
)

SELECT
    COUNT(*) < 1 AS _check,
    CASE WHEN COUNT(*) = 1 THEN 'There is ' ELSE 'There are ' END
    || COALESCE(COUNT(*), 0)
    || CASE WHEN COUNT(*) = 1 THEN ' unvalidated site.' ELSE ' unvalidated sites.'
    END AS summ,
    array_agg(descrip) AS gaps
FROM dedup