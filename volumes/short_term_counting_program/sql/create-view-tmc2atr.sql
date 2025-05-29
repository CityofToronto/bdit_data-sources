-- Transform turning movement counts into directed movements on legs
-- can't do this for peds :'-(

CREATE OR REPLACE VIEW traffic.tmc2atr AS

SELECT
    id,
    count_id,
    time_start,
    time_end,
    -- North leg traffic
    n_bike AS n_bike_sb,
    NULL::int AS n_bike_nb,
    n_cars_l + n_cars_t + n_cars_r AS n_cars_sb,
    s_cars_t + e_cars_r + w_cars_l AS n_cars_nb,
    n_truck_l + n_truck_t + n_truck_r AS n_truck_sb,
    s_truck_t + e_truck_r + w_truck_l AS n_truck_nb,
    -- South leg traffic
    s_bike AS s_bike_nb,
    NULL::int AS s_bike_sb,
    s_cars_t + s_cars_r + s_cars_t AS s_cars_nb,
    n_cars_t + e_cars_l + w_cars_r AS s_cars_sb,
    s_truck_t + s_truck_r + s_truck_t AS s_truck_nb,
    n_truck_t + e_truck_l + w_truck_r AS s_truck_sb,
    -- East leg traffic
    e_bike AS e_bike_wb,
    NULL::int AS e_bike_eb,
    e_cars_t + e_cars_r + e_cars_l AS e_cars_wb,
    w_cars_t + n_cars_l + s_cars_r AS e_cars_eb,
    e_truck_t + e_truck_r + e_truck_l AS e_truck_wb,
    w_truck_t + n_truck_l + s_truck_r AS e_truck_eb,
    -- West leg traffic
    w_bike AS w_bike_eb,
    NULL::int AS w_bike_wb,
    w_cars_t + w_cars_r + w_cars_l AS w_cars_eb,
    e_cars_t + s_cars_l + n_cars_r AS w_cars_wb,
    w_truck_t + w_truck_r + w_truck_l AS w_cars_eb,
    e_truck_t + s_truck_l + n_truck_r AS w_cars_wb
FROM traffic.tmc_study_data;
