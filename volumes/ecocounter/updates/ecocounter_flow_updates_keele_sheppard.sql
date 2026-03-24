-- There are validated flows with duplicate site_id, flow_direction, direction_main, mode_counted. 
-- Specifically, 4 sites near Keele/Shepphard and YorkU. site_id IN (300024437, 300024652, 300026120, 300026125)
-- dedupe these by re-assigning direction_main to make them main/contraflow.
-- Keep the flow directions the same, becuase they were validated using direction of travel,
-- and change the direction_main (side-of-path) of the lower-volume flows, assuming they are contraflow.
-- Flow geoms left unchanged since they are directional and this is not changing direction of travel

UPDATE ecocounter.flows_unfiltered AS fu
SET
    direction_main = vals.direction_main,
    notes = vals.notes
FROM (
    VALUES
-- 300024652 Keele St, north of Four Winds Dr (multi-use path)
    (353315315, 'Southbound'::gwolofs.travel_directions, 'Assumed to be contraflow based on volumes.'),
    (353315312, 'Northbound'::gwolofs.travel_directions, 'Assumed to be contraflow based on volumes.'),
-- 300026120 Keele St, south of Sheppard Ave W (multi-use path)
    (353324700, 'Southbound'::gwolofs.travel_directions, 'Assumed to be contraflow based on volumes.'),
    (353324701, 'Northbound'::gwolofs.travel_directions, 'Assumed to be contraflow based on volumes.'),
-- 300026125 Sheppard Ave W, west of Sentinel Rd (multi-use path)
    (353324721, 'Eastbound'::gwolofs.travel_directions, 'Assumed to be contraflow based on volumes.'),
    (353324720, 'Westbound'::gwolofs.travel_directions, 'Assumed to be contraflow based on volumes.'),
-- 300024437 Murray Ross Pkwy, north of Shoreham Dr (multi-use path)
    (353314057, 'Southbound'::gwolofs.travel_directions, 'Assumed to be contraflow based on volumes.'),
    (353314056, 'Northbound'::gwolofs.travel_directions, 'Assumed to be contraflow based on volumes.')
) AS vals(flow_id, direction_main, notes)
WHERE
    vals.flow_id = fu.flow_id;