--- Query to modify flow_direction to conform to the standard of direction main
ALTER TABLE temp_flows 
ALTER COLUMN flow_direction TYPE travel_directions USING TRIM(REGEXP_REPLACE(flow_direction, '\s*\((In|Out)\)$', ''))::travel_directions;

SELECT * FROM temp_flows;