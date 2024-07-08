CREATE OR REPLACE VIEW ecocounter.flows AS (
    SELECT
        flow_id,
        site_id,
        flow_direction,
        flow_geom,
        bin_size,
        notes,
        replaced_by_flow_id,
        replaces_flow_id,
        includes_contraflow,
        first_active,
        last_active
    FROM ecocounter.flows_unfiltered
    WHERE validated
);

ALTER VIEW ecocounter.flows OWNER TO ecocounter_admins;
GRANT ALL ON TABLE ecocounter.flows TO ecocounter_admins;

REVOKE ALL ON TABLE ecocounter.flows FROM bdit_humans;
GRANT SELECT ON TABLE ecocounter.flows TO bdit_humans;

GRANT SELECT ON TABLE ecocounter.flows TO ecocounter_bot;

COMMENT ON VIEW ecocounter.flows IS E''
'A flow is usually a direction of travel associated with a sensor at '
'an ecocounter installation site. For earlier sensors that did not detect '
'directed flows, a flow may be both directions of travel together, i.e. '
'just everyone who passed over the sensor any which way. '
'This table should only contain flows with at least some valid-ish data. ';

COMMENT ON COLUMN ecocounter.flows.bin_size
IS 'temporal bins are either 15, 30, or 60 minutes, depending on the sensor';

COMMENT ON COLUMN ecocounter.flows.flow_geom IS E''
'A two-node line, where the first node indicates the position of the sensor and '
'the second indicates the normal direction of travel over that sensor relative to the '
'first node. I.e. the line segment is an arrow pointing in the direction of travel.';

COMMENT ON COLUMN ecocounter.flows.includes_contraflow IS E''
'Does the flow also count travel in the reverse of the indicated flow direction? '
'TRUE indicates that the flow, though installed in one-way infrastucture like a standard bike '
'lane, also counts travel going the wrong direction within that lane.';

COMMENT ON COLUMN ecocounter.flows_unfiltered.first_active IS E''
'First timestamp flow_id appears in ecocounter.counts_unfiltered. '
'Updated using trigger with each insert on ecocounter.counts_unfiltered. ';

COMMENT ON COLUMN ecocounter.flows_unfiltered.last_active IS E''
'Last timestamp flow_id appears in ecocounter.counts_unfiltered. '
'Updated using trigger with each insert on ecocounter.counts_unfiltered. ';
