--- Query to modify flow_direction to conform to the standard of direction main
ALTER TABLE temp_flows 
ALTER COLUMN flow_direction TYPE travel_directions USING TRIM(REGEXP_REPLACE(flow_direction, '\s*\((In|Out)\)$', ''))::travel_directions;
SELECT * FROM temp_flows;

--- Query to add and initiate values
ALTER TABLE ecocounter.sites_unfiltered
ADD COLUMN bidirectional BOOLEAN default FALSE;

UPDATE ecocounter.sites_unfiltered
SET bidirectional = TRUE
	WHERE site_id IN (
		300026120, --- Keele St, south of Sheppard Ave W (multi-use path)
		300066323, --- Kipling Ave, north of Mt Olive Dr (multi-use path) 
		300066611, --- Lake Shore Blvd W, east of First St (two-way cycle track)
		300024652, --- Keele St, north of Four Winds Dr (multi-use path)
		300024486, --- Murray Ross Pkwy, west of Evelyn Wiggins Dr (multi-use path)
		300063317, --- Bayview Ave, south of Pottery Rd (multi-use path)
		300024437, --- Murray Ross Pkwy, north of Shoreham Dr (multi-use path)
		300026125, --- Sheppard Ave W, west of Sentinel Rd (multi-use path)
		300024488, --- Evelyn Wiggins Dr, north of Murray Ross Pkwy (two-way cycle track)
		300067807 --- Queens Quay W, west of Lower Simcoe St (two-way cycle track)
	)
;