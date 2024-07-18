CREATE VIEW ecocounter.combined_flows AS (
    WITH RECURSIVE recursive_flows AS (
        --flows which have never been replaced
        SELECT flow_id, replaced_by_flow_id
        FROM ecocounter.flows_unfiltered
        WHERE replaced_by_flow_id IS NULL

        UNION

        --flows replaced by those in above recursive CTE.
        SELECT
            f.flow_id,
            COALESCE(rf.replaced_by_flow_id, rf.flow_id)
        FROM ecocounter.flows_unfiltered AS f
        JOIN recursive_flows AS rf ON rf.flow_id = f.replaced_by_flow_id
    )

    --all flows and their current replacement
    SELECT
        flow_id,
        COALESCE(replaced_by_flow_id, flow_id) AS current_flow_id
    FROM recursive_flows
    ORDER BY flow_id
);

ALTER VIEW ecocounter.combined_flows OWNER TO ecocounter_admins;
GRANT ALL ON TABLE ecocounter.combined_flows TO ecocounter_admins;

REVOKE ALL ON TABLE ecocounter.combined_flows FROM bdit_humans;
GRANT SELECT ON TABLE ecocounter.combined_flows TO bdit_humans;

GRANT SELECT ON TABLE ecocounter.combined_flows TO ecocounter_bot;

COMMENT ON TABLE ecocounter.combined_flows
IS 'Recursively combine flow_ids which have been replaced
in order to get a continuous stream for monitoring project.';