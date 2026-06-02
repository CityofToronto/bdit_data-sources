UPDATE ecocounter.flows_unfiltered AS fu
SET
    flow_direction = vals.flow_direction,
    direction_main = vals.direction_main,
    includes_contraflow = vals.includes_contraflow,
    validated = vals.validated,
    mode_counted = vals.mode_counted
FROM (
    VALUES
    (353706300, 'Westbound', 'Eastbound'::gwolofs.travel_directions, TRUE,  FALSE, 'bike'),
    (353706299, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, FALSE, FALSE, 'bike'),
    (353706301, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, FALSE, FALSE, 'scooter'),
    (353706302, 'Westbound', 'Eastbound'::gwolofs.travel_directions, TRUE,  FALSE, 'scooter'),
    (353706303, 'Westbound', 'Westbound'::gwolofs.travel_directions, FALSE, FALSE, 'bike'),
    (353706304, 'Eastbound', 'Westbound'::gwolofs.travel_directions, TRUE,  FALSE, 'bike'),
    (353706305, 'Westbound', 'Westbound'::gwolofs.travel_directions, FALSE, FALSE, 'scooter'),
    (353706306, 'Eastbound', 'Westbound'::gwolofs.travel_directions, TRUE,  FALSE, 'scooter')
) AS vals(flow_id, flow_direction, direction_main, includes_contraflow, validated, mode_counted)
WHERE vals.flow_id = fu.flow_id;