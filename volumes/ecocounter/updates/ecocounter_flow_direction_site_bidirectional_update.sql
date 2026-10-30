--- Query to modify flow_direction to conform to the standard of direction main

--save and drop dependencies:
SELECT dbadmin.deps_save_and_drop_dependencies(
    p_view_schema:= 'ecocounter'::character varying COLLATE "C",
    p_view_name:= 'flows_unfiltered'::character varying COLLATE "C", 
    max_depth := 20::integer
);

UPDATE ecocounter.flows_unfiltered SET flow_direction = NULL WHERE initcap(flow_direction) = 'Unknown';

ALTER TABLE ecocounter.flows_unfiltered
ALTER COLUMN flow_direction TYPE travel_directions USING TRIM(REGEXP_REPLACE(flow_direction, '\s*\((In|Out)\)$', ''))::travel_directions;

--now restore dependencies:
SELECT dbadmin.deps_restore_dependencies(
    p_view_schema:= 'ecocounter'::character varying COLLATE "C",
    p_view_name:= 'flows_unfiltered'::character varying COLLATE "C"
);


--- Query to add and initiate values

--save and drop dependencies:
SELECT dbadmin.deps_save_and_drop_dependencies(
    p_view_schema:= 'ecocounter'::character varying COLLATE "C",
    p_view_name:= 'sites_unfiltered'::character varying COLLATE "C", 
    max_depth := 20::integer
);

ALTER TABLE ecocounter.sites_unfiltered
ADD COLUMN bidirectional BOOLEAN default FALSE;

--now restore dependencies:
SELECT dbadmin.deps_restore_dependencies(
    p_view_schema:= 'ecocounter'::character varying COLLATE "C",
    p_view_name:= 'sites_unfiltered'::character varying COLLATE "C"
);

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