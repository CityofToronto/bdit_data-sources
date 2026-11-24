-- View: ecocounter.active_flow_dates
-- ecocounter.active_flow_dates;

CREATE OR REPLACE VIEW ecocounter.active_flow_dates AS
WITH flow_info AS (
    --- full table of site_id, direction, flow_ids
    SELECT
        s.site_id,
        f.bin_size,
        f.flow_id,
        --- parse direction by flow_id
        CASE WHEN s.bidirectional THEN f.flow_direction ELSE f.direction_main END AS direction,
        ---a date range is used to include all flows in their respective active ranges
        daterange(
            f.first_active::date,
            COALESCE(f.date_decommissioned::date, CURRENT_DATE)
        ) AS flow_valid_range
    FROM ecocounter.sites AS s
    JOIN ecocounter.flows AS f USING (site_id)
),

listed_flow_dts AS (
    --- aggregate flow_id counts by date
    SELECT
        f.site_id,
        f.bin_size,
        f.direction,
        gs.valid_date::date,
        ARRAY_AGG(
            f.flow_id
            ORDER BY f.flow_id DESC
        ) AS flow_ids,
        COUNT(DISTINCT f.flow_id) AS n_flows
    FROM flow_info AS f,
        generate_series(
            lower(f.flow_valid_range), upper(f.flow_valid_range), '1 day'::interval
        ) AS gs (valid_date)
    GROUP BY
        f.site_id,
        f.direction,
        f.bin_size,
        gs.valid_date::date
)

SELECT
    site_id,
    bin_size,
    direction,
    flow_ids,
    n_flows,
    daterange(
        MIN(valid_date)::date,
        MAX(valid_date)::date,
        '[]'
    ) AS valid_range
FROM listed_flow_dts
GROUP BY (site_id, direction, flow_ids, n_flows, bin_size);

ALTER VIEW ecocounter.active_flow_dates OWNER TO ecocounter_admins;
GRANT ALL ON TABLE ecocounter.active_flow_dates TO ecocounter_admins;

REVOKE ALL ON TABLE ecocounter.active_flow_dates FROM bdit_humans;
GRANT SELECT ON TABLE ecocounter.active_flow_dates TO bdit_humans;

GRANT SELECT ON TABLE ecocounter.active_flow_dates TO ecocounter_bot;

COMMENT ON VIEW ecocounter.active_flow_dates
IS 'View containing an array and count of flows active for a given date range.
This is primarily used to validate whether all flows which are designated as active are receiving data on a given date';