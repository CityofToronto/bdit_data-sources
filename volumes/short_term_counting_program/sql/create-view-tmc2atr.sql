-- Transform turning movement counts into directed movements on legs
-- can't do this for peds, and can only do some of the counts for bikes

CREATE OR REPLACE VIEW traffic.tmc2atr_wide AS

SELECT
    id,
    count_id,
    time_start,
    time_end,
    -- North leg traffic
    n_bike AS n_bike_sb,
    -- if zero bikes enter the intersection, then zero bikes leave
    -- but if there is a non-zero bike count entering, we can't say
    -- where those bikes exit (yet), so the volume is NULL
    CASE
        WHEN w_bike + e_bike + s_bike = 0 THEN 0
    END AS n_bike_nb,
    n_cars_l + n_cars_t + n_cars_r AS n_cars_sb,
    s_cars_t + e_cars_r + w_cars_l AS n_cars_nb,
    n_truck_l + n_truck_t + n_truck_r AS n_truck_sb,
    s_truck_t + e_truck_r + w_truck_l AS n_truck_nb,
    -- South leg traffic
    s_bike AS s_bike_nb,
    CASE
        WHEN w_bike + e_bike + n_bike = 0 THEN 0
    END AS s_bike_sb,
    s_cars_t + s_cars_r + s_cars_t AS s_cars_nb,
    n_cars_t + e_cars_l + w_cars_r AS s_cars_sb,
    s_truck_t + s_truck_r + s_truck_t AS s_truck_nb,
    n_truck_t + e_truck_l + w_truck_r AS s_truck_sb,
    -- East leg traffic
    e_bike AS e_bike_wb,
    CASE
        WHEN w_bike + s_bike + n_bike = 0 THEN 0
    END AS e_bike_eb,
    e_cars_t + e_cars_r + e_cars_l AS e_cars_wb,
    w_cars_t + n_cars_l + s_cars_r AS e_cars_eb,
    e_truck_t + e_truck_r + e_truck_l AS e_truck_wb,
    w_truck_t + n_truck_l + s_truck_r AS e_truck_eb,
    -- West leg traffic
    w_bike AS w_bike_eb,
    CASE
        WHEN e_bike + s_bike + n_bike = 0 THEN 0
    END AS w_bike_wb,
    w_cars_t + w_cars_r + w_cars_l AS w_cars_eb,
    e_cars_t + s_cars_l + n_cars_r AS w_cars_wb,
    w_truck_t + w_truck_r + w_truck_l AS w_truck_eb,
    e_truck_t + s_truck_l + n_truck_r AS w_truck_wb
FROM traffic.tmc_study_data;

COMMENT ON VIEW traffic.tmc2atr_wide IS
'Mapping of short-term TMCs to ATR/SVC formatted counts on the legs of the intersection '
'For columns like `e_truck_wb`, e(East) is the leg with reference to the intersection '
'and wb(WestBound) is the direction of travel on that leg. `truck` is the type of vehicle.';
