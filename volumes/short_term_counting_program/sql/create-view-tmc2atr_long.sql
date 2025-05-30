CREATE OR REPLACE VIEW traffic.tmc2atr_long AS

WITH json_bundled AS (
    SELECT
        id,
        count_id,
        time_start,
        time_end,
        json_build_object(
            'bike', json_build_object(
                'n', json_build_object(
                    'nb', n_bike_nb,
                    'sb', n_bike_sb
                ),
                'e', json_build_object(
                    'eb', e_bike_eb,
                    'wb', e_bike_wb
                ),
                's', json_build_object(
                    'nb', s_bike_nb,
                    'sb', s_bike_sb
                ),
                'w', json_build_object(
                    'eb', w_bike_eb,
                    'wb', w_bike_wb
                )
            ),
            'car', json_build_object(
                'n', json_build_object(
                    'nb', n_cars_nb,
                    'sb', n_cars_sb
                ),
                'e', json_build_object(
                    'eb', e_cars_eb,
                    'wb', e_cars_wb
                ),
                's', json_build_object(
                    'nb', s_cars_nb,
                    'sb', s_cars_sb
                ),
                'w', json_build_object(
                    'eb', w_cars_eb,
                    'wb', w_cars_wb
                )
            ),
            'truck', json_build_object(
                'n', json_build_object(
                    'nb', n_truck_nb,
                    'sb', n_truck_sb
                ),
                'e', json_build_object(
                    'eb', e_truck_eb,
                    'wb', e_truck_wb
                ),
                's', json_build_object(
                    'nb', s_truck_nb,
                    'sb', s_truck_sb
                ),
                'w', json_build_object(
                    'eb', w_truck_eb,
                    'wb', w_truck_wb
                )
            )
        ) AS counts    
    FROM nwessel.tmc2atr
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