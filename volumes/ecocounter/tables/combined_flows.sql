WITH RECURSIVE flows_combined AS (
    SELECT
        ARRAY[flow_id] AS flow_ids
    FROM ecocounter.flows

    UNION

    SELECT
        array_append(flows_combined.flow_ids, flows.replaced_by_flow_id) AS flow_ids
    FROM ecocounter.flows
    JOIN flows_combined
        ON flows.flow_id = flows_combined.flow_ids[array_upper(flows_combined.flow_ids, 1)]
    WHERE flows.replaced_by_flow_id IS NOT NULL
)

SELECT flow_ids
FROM flows_combined AS a
WHERE
    NOT EXISTS (
        SELECT 1
        FROM flows_combined AS b
        WHERE
            -- overlaps another array/set
            a.flow_ids && b.flow_ids
            -- but is smaller than that array/set
            AND cardinality(a.flow_ids) < cardinality(b.flow_ids)
    )
ORDER BY flow_ids;
