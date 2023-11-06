--now find the gaps
WITH raw AS (
    SELECT
        {{ params.id_col }} AS sensor_id_col,
        {{ params.dt_col }} AS dt_col
    FROM {{ params.table }}
    WHERE
        {{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp
        AND {{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp + interval '1 day'
),

--add a start and end dt for each ID which appears in raw data
fluffed_data AS (
    SELECT DISTINCT
        raw.sensor_id_col,
        bins.dt_col,
        interval '0 minutes' AS gap_adjustment --don't need to reduce gap width for artificial data
    FROM raw, 
        (VALUES 
            ('{{ ds }} 00:00:00'::timestamp),
            ('{{ ds }} 00:00:00'::timestamp + interval '1 day')
        ) AS bins(dt_col)
    
    UNION ALL
    
    SELECT
        sensor_id_col,
        dt_col,
        --need to reduce gap length by 1 minute for real data since that minute contains data
        interval '1 minute' AS gap_adjustment
    FROM raw
),

--looks at sequential bins to identify gaps
bin_times AS (
    SELECT 
        sensor_id_col,
        dt_col AS gap_start,
        LEAD(dt_col, 1) OVER (PARTITION BY sensor_id_col ORDER BY dt_col) AS gap_end,
        LEAD(dt_col, 1) OVER (PARTITION BY sensor_id_col ORDER BY dt_col)
            - dt_col
            - SUM(gap_adjustment) --sum works because between 0 and 1, we want 1 (implies real data)
            AS bin_gap
    FROM fluffed_data
    GROUP BY
        sensor_id_col, 
        dt_col
) 

--summarize gaps by sensor.
SELECT
    sensor_id_col,
    STRING_AGG(gap_start || '--' || gap_end, '; ') AS gaps
FROM bin_times
WHERE
    gap_end IS NOT NULL --NULL gap_end occurs at the end of the search period
    AND bin_gap >= '{{ params.gap_threshold }}'::interval
GROUP BY sensor_id_col