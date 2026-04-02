
UPDATE ecocounter.flows_unfiltered
SET validated = TRUE
WHERE flow_id IN (

    -- 'Bloor St W, west of Huron St'
    353554898,353554900,353554897, -- westbound direction_main
    353341334, -- bike eastbound in westbound lane. Flag it as validated so it is included (even though it was not active on the day of the validation count)
    353341333,353554896,353554899,353554901, -- eastbound direction_main
    
    -- 'Bloor St E, west of Castle Frank Rd'
    353354428, -- eastbound direction_main
    353517542 -- westbound direction_main
);

UPDATE ecocounter.sites_unfiltered
SET validated = TRUE
WHERE site_id IN (
    -- 'Bloor St W, west of Huron St'
    300028396,
    -- 'Bloor St E, west of Castle Frank Rd'
    300030053
);
