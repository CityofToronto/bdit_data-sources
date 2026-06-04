UPDATE ecocounter.flows_unfiltered AS fu
SET
    flow_direction = vals.flow_direction,
    direction_main = vals.direction_main,
    includes_contraflow = FALSE,
    validated = FALSE,
    mode_counted = vals.mode_counted
FROM (
    VALUES
    (353665948, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, 'bike'),
    (353665949, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'bike'),
    (353665950, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'),
    (353665951, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'),
    (353665952, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353665953, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353665954, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'scooter'),
    (353665955, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'scooter')
) AS vals(flow_id, flow_direction, direction_main, mode_counted)
WHERE vals.flow_id = fu.flow_id;