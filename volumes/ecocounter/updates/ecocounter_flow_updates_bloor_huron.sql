-- CAUTION:
-- Flows 353341333 and 353341334 have valid data from 2022-10-27 to 2023-07-22. 
-- Starting July 2025 the hardware connected to these flow_ids was reconfigured:
-- * 353341333 was WB main+contraflow -> now it is EB Main flow (exclusively main flow)
-- * 353341334 was EB main+contraflow -> now it is EB in WB lane (exclusively contraflow)
-- This UPDATE will break the old 2022-2023 config and associated data. TBD how to handle flow_id configuration changs over time!!
-- Columns to update:
-- New values based on comparison w spectrum: flow_direction, direction_main, includes_contraflow=false, validated=false(it's in progress), notes
-- Re-assign based on changes: flow_geom, replaces_flow_id,
-- assign null: date decommissioned for flow_id 353341334, 
-- rough matching of maunal counts and expected peak hours saved: 
-- L:\TDCSB\PROJECT\Data Collection Team\03 Permanent Counters\EcoCounter\Analysis\2025-12-08 assigning directions to flows\compare spectrum to ecocoutner.xlsx

UPDATE ecocounter.flows_unfiltered AS fu
SET
    flow_direction = vals.flow_direction,
    direction_main = vals.direction_main,
    includes_contraflow = vals.includes_contraflow,
    validated = vals.validated,
    replaces_flow_id = vals.replaces_flow_id,
    date_decommissioned = vals.date_decommissioned,
    flow_geom = vals.flow_geom,
    notes = vals.notes
FROM (
    VALUES
    (353341333, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, FALSE, FALSE, 101042942, NULL::timestamp without time zone, '0102000020E6100000020000001F522D79C0D953C0A50989D25CD54540FBFD651EBDD953C084EC014C5ED54540'::geometry, 'bike - eastbound main (prior to 2025 this was westbound bike main+contraflow)'),
    (353554896, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, FALSE, FALSE, 101042942, NULL::timestamp without time zone, NULL, 'scooter - eastbound main'),
    (353554897, 'Eastbound', 'Westbound'::gwolofs.travel_directions, FALSE, FALSE, 101042942, NULL::timestamp without time zone, NULL, 'scooter - eastbound in westbound lane'),
    (353554898, 'Westbound', 'Westbound'::gwolofs.travel_directions, FALSE, FALSE, 104042942, NULL::timestamp without time zone, '0102000020E61000000200000054F06D51BED953C069071B8562D54540FA75B2C5C0D953C0E499BC7B61D54540'::geometry, 'bike - westbound main'),
    (353554899, 'Westbound', 'Eastbound'::gwolofs.travel_directions, FALSE, FALSE, 104042942, NULL::timestamp without time zone, NULL, 'bike - westbound in eastbound lane'),
    (353554900, 'Westbound', 'Westbound'::gwolofs.travel_directions, FALSE, FALSE, 104042942, NULL::timestamp without time zone, NULL, 'scooter - westbound main'),
    (353554901, 'Westbound', 'Eastbound'::gwolofs.travel_directions, FALSE, FALSE, 104042942, NULL::timestamp without time zone, NULL, 'scooter - westbound in eastbound lane'),
    (353341334, 'Eastbound', 'Westbound'::gwolofs.travel_directions, FALSE, FALSE, 101042942, NULL::timestamp without time zone, NULL, 'bike eastbound in westbound lane (prior to 2025 this was eastbound bike main+contraflow)')
) AS vals(flow_id, flow_direction, direction_main, includes_contraflow, validated, replaces_flow_id, date_decommissioned, flow_geom, notes)
WHERE
    vals.flow_id = fu.flow_id;

--add a new flow 53341333 to replace what was 353341333 before it was reused:
INSERT INTO ecocounter.flows_unfiltered (
    flow_id,
     site_id,
     flow_direction,
     flow_geom,
     bin_size,
     notes,
     replaced_by_flow_id,
     includes_contraflow,
     validated,
     first_active,
     last_active,
     date_decommissioned,
     direction_main,
     mode_counted
)
VALUES (
    53341333,
    300028396,
    'Westbound',
    '0102000020E6100000020000001F522D79C0D953C0A50989D25CD54540FBFD651EBDD953C084EC014C5ED54540',
    '00:15:00',
    'bike - eastbound main (prior to 2025 this was westbound bike main+contraflow). This flow_id was originally 353341333, but it was reused for the opposite direction so we changed the original to 53341333.',
    353554898,
    true,
    false,
    '2022-10-26 02:15:00',
    '2024-01-01 00:00:00',
    '2024-01-01 00:00:00',
    'Westbound',
    null
);

UPDATE ecocounter.counts_unfiltered
SET flow_id = 53341333
WHERE flow_id = 353341333 AND datetime_bin < '2024-01-01';

UPDATE ecocounter.sensitivity_history
SET flow_id = 53341333
WHERE flow_id = 353341333 AND lower(date_range) < '2024-01-01';

UPDATE ecocounter.flows_unfiltered
SET first_active = '2025-06-27 10:45:00'
WHERE flow_id = 353341333;
