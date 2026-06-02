UPDATE ecocounter.flows_unfiltered AS fu
SET
    flow_direction = vals.flow_direction,
    direction_main = vals.direction_main,
    includes_contraflow = FALSE,
    validated = FALSE,
    mode_counted = vals.mode_counted
FROM (
    VALUES
    (353706300, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'bike'),
    (353706299, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, 'bike'),
    (353706301, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'),
    (353706302, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'),
    (353706303, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353706304, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353706305, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'scooter'),
    (353706306, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'scooter')
) AS vals(flow_id, flow_direction, direction_main, mode_counted)
WHERE vals.flow_id = fu.flow_id;