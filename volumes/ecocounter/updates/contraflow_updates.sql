--Contraflow sites should have opposite direction_main (side of the road) and flow_direction (direction of travel). Fix that issue for any sites with "contraflow" in the name:
SELECT site_id, flow_id, flow_direction, direction_main, notes, mode_counted, includes_contraflow, substring(flow_direction, '^(\w+)')
FROM ecocounter.flows_unfiltered WHERE flow_direction LIKE '%contraflow%';

--these retired contraflow sites were ambiguous. Kept their direction_main and changed their flow_direction so data on open data does not change.
UPDATE ecocounter.flows_unfiltered SET flow_direction = 'Westbound' WHERE flow_id = 104042943;
UPDATE ecocounter.flows_unfiltered SET includes_contraflow = True WHERE flow_id = 101042943;
UPDATE ecocounter.flows_unfiltered SET flow_direction = 'Eastbound' WHERE flow_id = 102042943;
UPDATE ecocounter.flows_unfiltered SET flow_direction = 'Westbound' WHERE flow_id = 103042943;
UPDATE ecocounter.flows_unfiltered SET flow_direction = 'Northbound' WHERE flow_id = 101052525;

--these 4 have opposite directions. Removing the "(contraflow)"
UPDATE ecocounter.flows_unfiltered SET flow_direction = substring(flow_direction, '^(\w+)')
WHERE flow_id IN (353534021, 353534010, 353534008, 353534019)

--these two had same flow_direciton and direction_main but were labeled contraflow.
UPDATE ecocounter.flows_unfiltered SET flow_direction = 'Eastbound' WHERE flow_id = 102042942;
UPDATE ecocounter.flows_unfiltered SET flow_direction = 'Westbound' WHERE flow_id = 103042942;

--made cases consistent
UPDATE ecocounter.flows_unfiltered SET flow_direction = initcap(flow_direction)
