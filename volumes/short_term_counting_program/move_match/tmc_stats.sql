-- Find tmcs (this will become irrelevant - it's for temporary tesing purposes) 
WITH tmc_match AS (
    SELECT
        arterydata.arterycode,
        countinfomics.count_info_id,
        countinfomics.count_date
    FROM traffic.arterydata
    INNER JOIN traffic.countinfomics USING (arterycode)
    WHERE -- super short date range for testing purposes
        countinfomics.count_date >= '2023-01-01'
        AND countinfomics.count_date < '2023-01-15'
),

-- Add up vehicle totals in 15 minute bins (MOVE uses vehicle totals to calculate peak hours)
veh_tot AS (
    SELECT
        tmc_match.arterycode,
        tmc_match.count_info_id,
        det.count_time,
        (
            --cars
            det.n_cars_r + det.n_cars_t + det.n_cars_l
            + det.s_cars_r + det.s_cars_t + det.s_cars_l
            + det.e_cars_r + det.e_cars_t + det.e_cars_l
            + det.w_cars_r + det.w_cars_t + det.w_cars_l
            -- trucks
            + det.n_truck_r + det.n_truck_t + det.n_truck_l
            + det.s_truck_r + det.s_truck_t + det.s_truck_l
            + det.e_truck_r + det.e_truck_t + det.e_truck_l
            + det.w_truck_r + det.w_truck_t + det.w_truck_l
            -- buses
            + det.n_bus_r + det.n_bus_t + det.n_bus_l
            + det.s_bus_r + det.s_bus_t + det.s_bus_l
            + det.e_bus_r + det.e_bus_t + det.e_bus_l
            + det.w_bus_r + det.w_bus_t + det.w_bus_l
        ) AS veh_sum
    FROM tmc_match
    INNER JOIN traffic.det USING (count_info_id)
),

-- Add up rolling hourly totals 
hour_sums AS (
    SELECT
        count_info_id,
        arterycode,
        count_time,
        veh_sum,
        SUM(veh_sum) OVER (
            PARTITION BY count_info_id
            ORDER BY count_time
            RANGE BETWEEN '45 minutes' PRECEDING AND CURRENT ROW
        ) AS hour_tot
    FROM veh_tot
),

-- Determine the AM peak (using MOVE's methodology)
am_peak_ct AS (
    SELECT DISTINCT
        count_info_id,
        arterycode,
        FIRST_VALUE(count_time) OVER vol_desc AS am_peak_hr_start,
        FIRST_VALUE(hour_tot) OVER vol_desc AS am_peak_count
    FROM hour_sums
    WHERE
        count_time::time >= '07:30' -- as per MOVE
        AND count_time::time < '9:30' -- as per MOVE
        AND hour_tot IS NOT NULL
    WINDOW vol_desc AS (PARTITION BY count_info_id ORDER BY hour_tot DESC)
),

-- Determine the PM peak (using MOVE's methodology)
pm_peak_ct AS (
    SELECT DISTINCT
        count_info_id,
        arterycode,
        FIRST_VALUE(count_time) OVER vol_desc AS pm_peak_hr_start,
        FIRST_VALUE(hour_tot) OVER vol_desc AS pm_peak_count
    FROM hour_sums
    WHERE
        count_time::time >= '14:15' -- as per MOVE
        AND count_time::time < '18:00' --as per MOVE
        AND hour_tot IS NOT NULL
    WINDOW vol_desc AS (PARTITION BY count_info_id ORDER BY hour_tot DESC)
),

-- Union AM and PM peak times
peak_times AS (
    SELECT
        count_info_id,
        arterycode,
        am_peak_hr_start - interval '1 HOUR' AS peak_hour_start,
        am_peak_hr_start AS peak_hour_end
    FROM am_peak_ct

    UNION ALL

    SELECT
        count_info_id,
        arterycode,
        pm_peak_hr_start - interval '1 HOUR' AS peak_hour_start,
        pm_peak_hr_start AS peak_hour_end
    FROM pm_peak_ct
)

-- Grab counts for peak hours by mode
SELECT
    peak_times.count_info_id,
    peak_times.arterycode,
    CONCAT(arterydata.street1, ' and ', arterydata.street2) AS study_loc,
    peak_times.peak_hour_start,
    peak_times.peak_hour_end,
    sum(det.n_cars_r) AS sb_cars_r,
    sum(det.n_cars_t) AS sb_cars_t,
    sum(det.n_cars_l) AS sb_cars_l,
    sum(det.s_cars_r) AS nb_cars_r,
    sum(det.s_cars_t) AS nb_cars_t,
    sum(det.s_cars_l) AS nb_cars_l,
    sum(det.e_cars_r) AS wb_cars_r,
    sum(det.e_cars_t) AS wb_cars_t,
    sum(det.e_cars_l) AS wb_cars_l,
    sum(det.w_cars_r) AS eb_cars_r,
    sum(det.w_cars_t) AS eb_cars_t,
    sum(det.w_cars_l) AS eb_cars_l,
    sum(det.n_truck_r) AS sb_truck_r,
    sum(det.n_truck_t) AS sb_truck_t,
    sum(det.n_truck_l) AS sb_truck_l,
    sum(det.s_truck_r) AS nb_truck_r,
    sum(det.s_truck_t) AS nb_truck_t,
    sum(det.s_truck_l) AS nb_truck_l,
    sum(det.e_truck_r) AS wb_truck_r,
    sum(det.e_truck_t) AS wb_truck_t,
    sum(det.e_truck_l) AS wb_truck_l,
    sum(det.w_truck_r) AS eb_truck_r,
    sum(det.w_truck_t) AS eb_truck_t,
    sum(det.w_truck_l) AS eb_truck_l,
    sum(det.n_bus_r) AS sb_bus_r,
    sum(det.n_bus_t) AS sb_bus_t,
    sum(det.n_bus_l) AS sb_bus_l,
    sum(det.s_bus_r) AS nb_bus_r,
    sum(det.s_bus_t) AS nb_bus_t,
    sum(det.s_bus_l) AS nb_bus_l,
    sum(det.e_bus_r) AS wb_bus_r,
    sum(det.e_bus_t) AS wb_bus_t,
    sum(det.e_bus_l) AS wb_bus_l,
    sum(det.w_bus_r) AS eb_bus_r,
    sum(det.w_bus_t) AS eb_bus_t,
    sum(det.w_bus_l) AS eb_bus_l,
    sum(det.n_peds) AS sb_peds,
    sum(det.s_peds) AS nb_peds,
    sum(det.e_peds) AS wb_peds,
    sum(det.w_peds) AS eb_peds,
    sum(det.n_bike) AS sb_bike,
    sum(det.s_bike) AS nb_bike,
    sum(det.e_bike) AS wb_bike,
    sum(det.w_bike) AS eb_bike,
    sum(det.n_other) AS sb_other,
    sum(det.s_other) AS nb_other,
    sum(det.e_other) AS wb_other,
    sum(det.w_other) AS eb_other
FROM peak_times
INNER JOIN traffic.det
    ON det.count_info_id = peak_times.count_info_id
    AND det.count_time > peak_times.peak_hour_start
    AND det.count_time <= peak_times.peak_hour_end
LEFT JOIN traffic.arterydata USING (arterycode)
GROUP BY
    peak_times.count_info_id,
    peak_times.arterycode,
    peak_times.peak_hour_start,
    peak_times.peak_hour_end,
    arterydata.street1,
    arterydata.street2
ORDER BY
    peak_times.count_info_id,
    peak_times.arterycode,
    peak_times.peak_hour_start,
    peak_times.peak_hour_end;