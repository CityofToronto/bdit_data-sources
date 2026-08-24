UPDATE ecocounter.flows_unfiltered AS fu
SET
    flow_direction = vals.flow_direction,
    direction_main = vals.direction_main,
    includes_contraflow = FALSE,
    validated = FALSE,
    mode_counted = vals.mode_counted
FROM (
    VALUES
    (353706545, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'bike'), -- correct adelaide: includes_contraflow should be FALSE
    (353706547, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'), -- correct adelaide: includes_contraflow should be FALSE
    -- 300063317    "Bayview Ave, south of Pottery Rd (multi-use path)" 
    (353665993, 'Northbound', 'Northbound'::gwolofs.travel_directions, 'bike'),
    (353665994, 'Northbound', 'Northbound'::gwolofs.travel_directions, 'scooter'),
    (353665995, 'Southbound', 'Southbound'::gwolofs.travel_directions, 'bike'), -- unsure
    (353665996, 'Southbound', 'Southbound'::gwolofs.travel_directions, 'scooter'),
    (353749736, 'Northbound', 'Southbound'::gwolofs.travel_directions, 'bike'),
    (353749738, 'Northbound', 'Southbound'::gwolofs.travel_directions, 'scooter'),
    (353749740, 'Southbound', 'Northbound'::gwolofs.travel_directions, 'bike'), -- unsure 
    (353749742, 'Southbound', 'Northbound'::gwolofs.travel_directions, 'scooter'),
    -- 300066342    "Chesswood Dr, north of Vanley Cres" (NB)
    (353705366, 'Northbound', 'Northbound'::gwolofs.travel_directions, 'bike'),
    (353705367, 'Southbound', 'Northbound'::gwolofs.travel_directions, 'bike'),
    (353705368, 'Northbound', 'Northbound'::gwolofs.travel_directions, 'scooter'),
    (353705369, 'Southbound', 'Northbound'::gwolofs.travel_directions, 'scooter'),
    -- 300066343    "Chesswood Dr, north of Vanley Cres" (SB)
    (353705377, 'Southbound', 'Southbound'::gwolofs.travel_directions, 'bike'),
    (353705378, 'Northbound', 'Southbound'::gwolofs.travel_directions, 'bike'),
    (353705379, 'Southbound', 'Southbound'::gwolofs.travel_directions, 'scooter'),
    (353705380, 'Northbound', 'Southbound'::gwolofs.travel_directions, 'scooter'),
    -- 300066509	"College St, east of Major St" (WB)
    (353706559, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353706560, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353706561, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'scooter'),
    (353706562, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'scooter'),
    -- 300066510	"College St, east of Major St" (EB)
    (353706570, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, 'bike'),
    (353706571, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'bike'),
    (353706572, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'),
    (353706573, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'),
    -- 300066427	"Dundas St E, west of Jones Ave" (EB)
    (353706031, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353706032, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353706033, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'scooter'),
    (353706034, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'scooter'),
    -- 300066428	"Dundas St E, west of Jones Ave" (WB)
    (353706042, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, 'bike'),
    (353706043, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'bike'),
    (353706044, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'),
    (353706045, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'),
    -- 300066512	"Gerrard St, west of Yonge St" (EB)
    (353706603, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, 'bike'),
    (353706604, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'bike'),
    (353706605, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'),
    (353706606, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'),
    -- 300070251	"Gerrard St, west of Yonge St" (WB)
    (353755145, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353755146, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353755160, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'scooter'),
    (353755163, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'scooter'),
    -- 300066323	"Kipling Ave, north of Mt Olive Dr (multi-use path) "
    (353705221, 'Southbound', 'Southbound'::gwolofs.travel_directions, 'bike'), -- no contraflow sensor apparent
    (353705222, 'Southbound', 'Southbound'::gwolofs.travel_directions, 'scooter'), -- no contraflow sensor apparent
    (353705223, 'Northbound', 'Northbound'::gwolofs.travel_directions, 'bike'), -- no contraflow sensor apparent
    (353705224, 'Northbound', 'Northbound'::gwolofs.travel_directions, 'scooter'), -- no contraflow sensor apparent
    -- 300066611	"Lake Shore Blvd W, east of First St (two-way cycle track)"
    -- these are unclear. might need to be changed after comparing to validation count
    (353708701, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, 'bike'),
    (353708702, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'bike'),
    (353708703, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'),
    (353708704, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'),
    (353708705, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353708706, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353708707, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'scooter'),
    (353708708, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'scooter'),
    -- 300067807	"Queens Quay W, west of Lower Simcoe St (two-way cycle track)"
    (353724474, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353724475, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'scooter'),
    (353724476, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'bike'),
    (353724477, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, 'bike'),
    (353724478, 'Westbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'),
    (353724479, 'Eastbound', 'Eastbound'::gwolofs.travel_directions, 'scooter'),
    (353725725, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353725727, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'scooter'),
    -- 300067687	"Richmond St W, west of Simcoe St"
    (353722535, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353722536, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'bike'),
    (353722537, 'Eastbound', 'Westbound'::gwolofs.travel_directions, 'scooter'),
    (353722538, 'Westbound', 'Westbound'::gwolofs.travel_directions, 'scooter'),
    -- 300066610	"Shaw St, north of Essex St (South Intersection)"
    
    -- 300066346	"Sheppard Ave E, east of Willowdale Ave"
    -- 300066351	"Sheppard Ave E, east of Willowdale Ave"
    -- 300028589	"Sherbourne St, north of Wellesley St E"
    -- 300066608	"Shuter St, west of Sherbourne St"
    -- 300066609	"Shuter St, west of Sherbourne St"
    -- 300062142	"Steeles Ave E, east of McCowan Rd"
    -- 300060828	"Steeles Ave E, east of Midland Ave"
    -- 300062143	"Steeles Ave E, east of Midland Ave"
    -- 300062141	"Steeles Ave E, west of McCowan Rd"
    -- 300066511	"Wellesley St W, west of Queen's Park Cres E"
    -- 300066423	"Woodbine Ave, north of Kingston Rd"
    -- 300066426	"Woodbine Ave, north of Kingston Rd"


) AS vals(flow_id, flow_direction, direction_main, mode_counted)
WHERE vals.flow_id = fu.flow_id;

-- 300066512	"Gerrard St, west of Yonge St" flows no longer used
-- These contain westbound cyclist main-flow and contraflow data from the installation date up to '2026-07-28 08:00:00’
UPDATE ecocounter.flows_unfiltered AS fu
SET date_decommissioned = '2026-07-28 08:00:00'
WHERE flow_id IN (353750135, 353750136);

-- 300066323	"Kipling Ave, north of Mt Olive Dr (multi-use path) " no contraflow sensor apparent
UPDATE ecocounter.flows_unfiltered AS fu
SET includes_contraflow = TRUE
WHERE flow_id IN (
    353705221,
    353705222,
    353705223,
    353705224
);