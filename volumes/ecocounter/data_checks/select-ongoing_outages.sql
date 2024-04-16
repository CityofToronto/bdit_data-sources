WITH out_of_order AS (
    SELECT
        s.site_id,
        s.site_description,
        f.flow_id,
        s.site_description || ' (site_id: ' || s.site_id
            || (CASE WHEN f.flow_id IS NOT NULL THEN ', channel_id: ' || f.flow_Id ELSE '' END)
            || ') - data last received: ' || MAX(c.datetime_bin::date)
            || ' (' || '{{ ds }} 00:00:00'::timestamp - MAX(c.datetime_bin::date) || ')' AS description
    FROM ecocounter.counts AS c --only validated sites
    JOIN ecocounter.flows AS f USING (flow_id)
    JOIN ecocounter.sites AS s USING (site_id)
    WHERE
        c.datetime_bin >= '{{ ds }} 00:00:00'::timestamp - interval '{{ params.lookback }}'
        AND c.datetime_bin < '{{ ds }} 00:00:00'::timestamp + interval '1 day'
    GROUP BY
        --find both sites and site/flow combos which are out of order.
        GROUPING SETS ((s.site_id), (s.site_id, f.flow_id)),
        s.site_description
    HAVING MAX(c.datetime_bin::date) < '{{ ds }} 00:00:00'::timestamp - interval '{{ params.min_duration }}'

    EXCEPT

    SELECT * FROM
        (VALUES (300031255::numeric, 'Bloor St W, between Palmerston & Markham'::text, null::numeric, 'Bloor St W, between Palmerston & Markham (site_id: 300031255) - data last received: 2024-04-11 (4 days)'::text))
    AS test_exclusion
),

ongoing_outages AS (
    --out of order sites (all channels)
    SELECT ooo_sites.description
    FROM out_of_order AS ooo_sites
    WHERE ooo_sites.flow_id IS NULL

    UNION

    --out of order flows (where channel does not have overall outage)
    SELECT ooo_flows.description
    FROM out_of_order AS ooo_flows
    --anti join ooo_sites
    LEFT JOIN out_of_order AS ooo_sites
        ON ooo_flows.site_id = ooo_sites.site_id
        AND ooo_sites.flow_id IS NULL
    WHERE
        ooo_flows.flow_id IS NOT NULL
        AND ooo_sites.site_id IS NULL --anti join
)
    
SELECT
    COUNT(ongoing_outages.*) < 1 AS check,
    CASE WHEN COUNT(ongoing_outages.*) = 1 THEN 'There is ' ELSE 'There are ' END ||
        COALESCE(COUNT(ongoing_outages.*), 0) ||
        CASE WHEN COUNT(ongoing_outages.*) = 1 THEN ' ongoing outage.' ELSE ' ongoing outages.'
    END AS summ,
    array_agg(ongoing_outages.description || chr(10)) AS gaps
FROM ongoing_outages