CREATE OR REPLACE VIEW traffic.tmc2atr AS

WITH tmc2atr_wide AS (
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
    FROM traffic.tmc_study_data
),

json_bundled AS (
    SELECT
        id,
        count_id,
        time_start,
        time_end,
        json_build_object(
            'bike', json_build_object(
                'N', json_build_object(
                    'NB', n_bike_nb,
                    'SB', n_bike_sb
                ),
                'E', json_build_object(
                    'EB', e_bike_eb,
                    'WB', e_bike_wb
                ),
                'S', json_build_object(
                    'NB', s_bike_nb,
                    'SB', s_bike_sb
                ),
                'W', json_build_object(
                    'EB', w_bike_eb,
                    'WB', w_bike_wb
                )
            ),
            'car', json_build_object(
                'N', json_build_object(
                    'NB', n_cars_nb,
                    'SB', n_cars_sb
                ),
                'E', json_build_object(
                    'EB', e_cars_eb,
                    'WB', e_cars_wb
                ),
                'S', json_build_object(
                    'NB', s_cars_nb,
                    'SB', s_cars_sb
                ),
                'W', json_build_object(
                    'EB', w_cars_eb,
                    'WB', w_cars_wb
                )
            ),
            'truck', json_build_object(
                'N', json_build_object(
                    'NB', n_truck_nb,
                    'SB', n_truck_sb
                ),
                'E', json_build_object(
                    'EB', e_truck_eb,
                    'WB', e_truck_wb
                ),
                'S', json_build_object(
                    'NB', s_truck_nb,
                    'SB', s_truck_sb
                ),
                'W', json_build_object(
                    'EB', w_truck_eb,
                    'WB', w_truck_wb
                )
            )
        ) AS counts
    FROM tmc2atr_wide
),

classified AS (
    SELECT
        id,
        count_id,
        time_start,
        time_end,
        (json_each(counts)).key AS classification,
        (json_each(counts)).value AS counts
    FROM json_bundled
),

legged AS (
    SELECT
        id,
        count_id,
        time_start,
        time_end,
        classification,
        (json_each(counts)).key AS leg,
        (json_each(counts)).value AS counts
    FROM classified
),

directed_text AS (
    SELECT
        id,
        count_id,
        time_start,
        time_end,
        classification,
        leg,
        (json_each(counts)).key AS dir,
        -- have to first parse JSON as text
        ((json_each(counts)).value)::text AS count_text
    FROM legged
)

SELECT
    id,
    count_id,
    time_start,
    time_end,
    classification,
    leg,
    dir,
    CASE
        WHEN count_text ~ '\d' THEN count_text::smallint
        -- 'null' text strings will become actual nulls
    END AS count
FROM directed_text;

COMMENT ON VIEW traffic.tmc2atr
IS 'Mapping of short-term TMCs to ATR/SVC-formatted counts on the legs of the intersection';
