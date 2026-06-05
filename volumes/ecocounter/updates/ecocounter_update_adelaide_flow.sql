UPDATE ecocounter.flows_unfiltered AS fu
SET
    flow_direction = vals.flow_direction,
    direction_main = vals.direction_main,
    includes_contraflow = vals.includes_contraflow,
    validated = FALSE,
    mode_counted = vals.mode_counted
FROM (
    VALUES
    (353706545, 'Westbound', 'Eastbound'::gwolofs.travel_directions, TRUE,  'bike'),
    (353706546, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, FALSE, 'bike'),
    (353706547, 'Westbound', 'Eastbound'::gwolofs.travel_directions, TRUE,  'scooter'),
    (353706548, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, FALSE, 'scooter')
) AS vals(flow_id, flow_direction, direction_main, includes_contraflow, mode_counted)
WHERE vals.flow_id = fu.flow_id;