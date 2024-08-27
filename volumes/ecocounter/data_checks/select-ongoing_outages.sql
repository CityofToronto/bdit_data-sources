WITH ooo_sites AS (
    SELECT
        site_id,
        site_description,
        last_active
    FROM ecocounter.sites
    WHERE
        last_active < CURRENT_DATE - 1
        AND date_decommissioned IS NULL
),

ooo_flows_and_sites AS (
    SELECT
        s.site_id,
        s.site_description,
        f.flow_id,
        f.last_active
    FROM ecocounter.flows AS f
    JOIN ecocounter.sites AS s USING (site_id)
    --anti join ooo_sites
    LEFT JOIN ooo_sites USING (site_id)
    WHERE
        ooo_sites.site_id IS NULL
        AND f.last_active < CURRENT_DATE - 1
        AND f.date_decommissioned IS NULL
    UNION
    SELECT
        site_id,
        site_description,
        NULL AS flow_id,
        last_active
    FROM ooo_sites
)

SELECT
    COUNT(*) < 1 AS _check,
    CASE WHEN COUNT(*) = 1 THEN 'There is ' ELSE 'There are ' END || COALESCE(COUNT(*), 0)
    || CASE WHEN COUNT(*) = 1 THEN ' ongoing outage.' ELSE ' ongoing outages.' END AS summ,
    array_agg(
        'Site: `' || site_description || ' (site_id: ' || site_id
        || (CASE WHEN flow_id IS NOT NULL THEN ', channel_id: ' || flow_id ELSE '' END)
        || ')` - date last received: `' || last_active::date
        || ' (' || CURRENT_DATE - 1 - last_active::date || ' days)`'
    ) AS gaps
FROM ooo_flows_and_sites