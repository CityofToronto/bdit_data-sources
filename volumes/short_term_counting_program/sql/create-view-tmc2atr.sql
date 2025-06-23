/*
if zero bikes enter the intersection, then zero bikes leave
but if there is a non-zero bike count entering, we can't say
where those bikes exit (yet), so the volume is NULL
*/

CREATE OR REPLACE VIEW traffic.tmc2atr AS

WITH json_bundled AS (
    SELECT
        id,
        count_id,
        time_start,
        time_end,
        json_build_object( -- classification level
            'bike', json_build_object( -- approach level
                'N', json_build_object( -- direction level
                    'NB', CASE WHEN w_bike + e_bike + s_bike = 0 THEN 0 END,
                    'SB', n_bike
                ),
                'E', json_build_object(
                    'EB', CASE WHEN w_bike + s_bike + n_bike = 0 THEN 0 END,
                    'WB', e_bike
                ),
                'S', json_build_object(
                    'NB', s_bike,
                    'SB', CASE WHEN w_bike + e_bike + n_bike = 0 THEN 0 END
                ),
                'W', json_build_object(
                    'EB', w_bike,
                    'WB', CASE WHEN e_bike + s_bike + n_bike = 0 THEN 0 END
                )
            ),
            'car', json_build_object(
                'N', json_build_object(
                    'NB', s_cars_t + e_cars_r + w_cars_l,
                    'SB', n_cars_l + n_cars_t + n_cars_r
                ),
                'E', json_build_object(
                    'EB', w_cars_t + n_cars_l + s_cars_r,
                    'WB', e_cars_t + e_cars_r + e_cars_l
                ),
                'S', json_build_object(
                    'NB', s_cars_t + s_cars_r + s_cars_t,
                    'SB', n_cars_t + e_cars_l + w_cars_r
                ),
                'W', json_build_object(
                    'EB', w_cars_t + w_cars_r + w_cars_l,
                    'WB', e_cars_t + s_cars_l + n_cars_r
                )
            ),
            'truck', json_build_object(
                'N', json_build_object(
                    'NB', s_truck_t + e_truck_r + w_truck_l,
                    'SB', n_truck_l + n_truck_t + n_truck_r
                ),
                'E', json_build_object(
                    'EB', w_truck_t + n_truck_l + s_truck_r,
                    'WB', e_truck_t + e_truck_r + e_truck_l
                ),
                'S', json_build_object(
                    'NB', s_truck_t + s_truck_r + s_truck_t,
                    'SB', n_truck_t + e_truck_l + w_truck_r
                ),
                'W', json_build_object(
                    'EB', w_truck_t + w_truck_r + w_truck_l,
                    'WB', e_truck_t + s_truck_l + n_truck_r
                )
            )
        ) AS counts
    FROM traffic.tmc_study_data
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
