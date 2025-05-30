CREATE OR REPLACE VIEW traffic.tmc2atr_long AS

WITH json_bundled AS (
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
    FROM nwessel.tmc2atr_wide
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

COMMENT ON traffic.tmc2atr_long IS 'Long-formatted version of `tmc2atr`.'