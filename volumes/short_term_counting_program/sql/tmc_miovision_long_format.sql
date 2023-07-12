/*
This view transforms traffic.det's wide-format TMC table into a 
long-format table that's more compatible with the way we store 
Miovision TMCs, as in miovision_api.volumes_15min_tmc.
*/

CREATE OR REPLACE VIEW traffic.tmc_miovision_long_format AS

WITH trans AS (
    SELECT
        count_info_id,
        -- because traffic, unlike miovision indicates count time as
        -- the end rather than start of the 15 minute bin
        count_time::time - '15 minutes'::interval AS time_bin,
        json_build_object(
            'n_cars_r', n_cars_r,
            'n_cars_t', n_cars_t,
            'n_cars_l', n_cars_l,
            's_cars_r', s_cars_r,
            's_cars_t', s_cars_t,
            's_cars_l', s_cars_l,
            'e_cars_r', e_cars_r,
            'e_cars_t', e_cars_t,
            'e_cars_l', e_cars_l,
            'w_cars_r', w_cars_r,
            'w_cars_t', w_cars_t,
            'w_cars_l', w_cars_l,
            --
            'n_truck_r', n_truck_r,
            'n_truck_t', n_truck_t,
            'n_truck_l', n_truck_l,
            's_truck_r', s_truck_r,
            's_truck_t', s_truck_t,
            's_truck_l', s_truck_l,
            'e_truck_r', e_truck_r,
            'e_truck_t', e_truck_t,
            'e_truck_l', e_truck_l,
            'w_truck_r', w_truck_r,
            'w_truck_t', w_truck_t,
            'w_truck_l', w_truck_l,
            --
            'n_bus_r', n_bus_r,
            'n_bus_t', n_bus_t,
            'n_bus_l', n_bus_l,
            's_bus_r', s_bus_r,
            's_bus_t', s_bus_t,
            's_bus_l', s_bus_l,
            'e_bus_r', e_bus_r,
            'e_bus_t', e_bus_t,
            'e_bus_l', e_bus_l,
            'w_bus_r', w_bus_r,
            'w_bus_t', w_bus_t,
            'w_bus_l', w_bus_l,
            --
            'n_peds', n_peds,
            's_peds', s_peds,
            'e_peds', e_peds,
            'w_peds', w_peds,
            --
            'n_bike', n_bike,
            's_bike', s_bike,
            'e_bike', e_bike,
            'w_bike', w_bike,
            --
            'n_other', n_other,
            's_other', s_other,
            'e_other', e_other,
            'w_other', w_other
        ) AS tmc
    FROM traffic.det
),

unpacked AS (
    SELECT
        count_info_id,
        time_bin,
        (json_each(tmc)).key AS kee,
        substring((json_each(tmc)).key, '_([rtl])$') AS movement,
        substring((json_each(tmc)).key, '^._(cars|truck|bus|peds|bike|other)') AS trans_mode,
        (json_each(tmc)).value::text AS volume -- can't cast a JSON type directly to integer
    FROM trans
)

SELECT
    countinfomics.arterycode,
    countinfomics.count_date::date + u.time_bin AS datetime_bin,
    -- fun fact: 'KEY' is a ... KEYword. Thus: 'kee'
    upper(substring(u.kee, '^([nsew])_')) AS leg,
    CASE
        WHEN u.movement = 't' THEN 1
        WHEN u.movement = 'l' THEN 2
        WHEN u.movement = 'r' THEN 3
        WHEN u.trans_mode = 'bike' THEN 7
        -- modes "other" and "peds" are left undefined, though peds 
        -- actually combines movement_uid's 5 and 6
    END AS movement_uid,
    CASE
        WHEN u.trans_mode = 'cars' THEN 1
        WHEN u.trans_mode = 'peds' THEN 6
        WHEN u.trans_mode = 'bike' THEN 10
        WHEN u.trans_mode = 'bus' THEN 3
        WHEN u.trans_mode = 'truck' THEN 4 -- open to debate, this one
        -- again, other undefined
    END AS classification_uid,
    CASE -- necessary because there's a sneaky 'NULL' hiding somewhere
        WHEN u.volume ~ '^\d+$' THEN u.volume::int
    END AS volume
FROM unpacked AS u
JOIN traffic.countinfomics USING (count_info_id);

GRANT SELECT ON traffic.tmc_miovision_long_format TO bdit_humans;
