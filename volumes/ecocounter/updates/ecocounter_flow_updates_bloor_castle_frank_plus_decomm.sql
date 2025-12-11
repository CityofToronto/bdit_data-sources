--these flow_ids have never produced data
UPDATE ecocounter.flows_unfiltered
SET date_decommissioned = '2025-12-11'
WHERE flow_id IN (353455984, 353629056, 353629057, 353629035, 353629036);

-- CAUTION:
-- Two Flows at Bloor St E, west of Castle Frank Rd are incorrect as of 2025-12-09:
-- * 353354428 was labelled Westbound, it is actually Eastbound
-- * 353517542 was labelled Eastbound, it is actually Westbound

-- Columns to update:
-- New values based on comparison w spectrum counts and peak period trends: flow_direction, direction_main, includes_contraflow, validated=false(it's in progress)
-- Re-assign based on changes: flow_geom

-- rough matching of maunal counts and expected peak hours saved: 
-- L:\TDCSB\PROJECT\Data Collection Team\03 Permanent Counters\EcoCounter\Analysis\2025-12-08 assigning directions to flows\compare spectrum to ecocoutner.xlsx

UPDATE ecocounter.flows_unfiltered AS fu
SET
    flow_direction = vals.flow_direction,
    direction_main = vals.direction_main,
    includes_contraflow = vals.includes_contraflow,
    validated = vals.validated,
    replaces_flow_id = vals.replaces_flow_id,
    flow_geom = vals.flow_geom,
    notes = vals.notes
FROM (
    VALUES
    (353354428, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, TRUE, FALSE, '0102000020E610000002000000C92B4F2992D753C05B0F2A1637D64540BD7592D38ED753C06D6376AE3BD64540'::geometry),
    (353517542, 'Westbound', 'Westbound'::gwolofs.travel_directions, TRUE, FALSE, '0102000020E610000002000000F8DFF5D796D753C0B667FED438D64540B3ABFC3D9AD753C0290E7D4E34D64540'::geometry)
) AS vals(flow_id, flow_direction, direction_main, includes_contraflow, validated, flow_geom)
WHERE
    vals.flow_id = fu.flow_id;