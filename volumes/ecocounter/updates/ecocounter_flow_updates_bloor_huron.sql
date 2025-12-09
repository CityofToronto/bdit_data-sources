-- based on rough matching of maunal counts and expected peak hours saved:
-- L:\TDCSB\PROJECT\Data Collection Team\03 Permanent Counters\EcoCounter\Analysis\2025-12-08 assigning directions to flows\compare spectrum to ecocoutner.xlsx
-- also remove date decommissioned for flow_id 353341334, remove flow geoms and replaces_flow_id since they have changed?
UPDATE ecocounter.flows_unfiltered AS fu
SET
    flow_direction = vals.flow_direction,
    direction_main = vals.direction_main,
    includes_contraflow = vals.includes_contraflow
FROM (
    VALUES
    (353341333, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, FALSE),
    (353554896, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, FALSE),
    (353554897, 'Eastbound', 'Westbound'::gwolofs.travel_directions, FALSE),
    (353554898, 'Westbound', 'Westbound'::gwolofs.travel_directions, FALSE),
    (353554899, 'Westbound', 'Eastbound'::gwolofs.travel_directions, FALSE),
    (353554900, 'Westbound', 'Westbound'::gwolofs.travel_directions, FALSE),
    (353554901, 'Westbound', 'Eastbound'::gwolofs.travel_directions, FALSE),
    (353341334, 'Eastbound', 'Westbound'::gwolofs.travel_directions, FALSE)
) AS vals(flow_id, flow_direction, direction_main, includes_contraflow)
WHERE
    vals.flow_id = fu.flow_id;