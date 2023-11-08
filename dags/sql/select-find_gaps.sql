--a generic find_gaps function to find gaps in sensor data
--includes a lookback of `gap_threshold` so it catches gaps overlapping the start of the interval
--includes synthetic start/end time points for all sensors so we catch gaps that don't start or end in the interval
    --doesn't catch sensors with no data in the interval. 
--noqa: disable=TMP, PRS

WITH raw AS (
    SELECT DISTINCT
        {{ params.id_col }} AS sensor_id_col,
        {{ params.dt_col }} AS dt_col
    FROM {{ params.table }}
    WHERE
        {{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp - '{{ params.gap_threshold }}'::interval -- eg. >= 23:00
        AND {{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp + '1 day'::interval
),

--add a start and end dt for each ID which appears in raw data
fluffed_data AS (
    --synthetic data
    SELECT DISTINCT
        raw.sensor_id_col,
        bins.dt_col,
        --don't need to reduce gap width for synthetic data since that minute does not contain data
        '0 minutes'::interval AS gap_adjustment
    FROM raw, 
        (VALUES 
            ('{{ ds }} 00:00:00'::timestamp - '{{ params.gap_threshold }}'::interval),
            ('{{ ds }} 00:00:00'::timestamp + '1 day'::interval)
        ) AS bins(dt_col)
    
    UNION ALL
    
    SELECT
        sensor_id_col,
        dt_col,
        --need to reduce gap length by 1 minute for real data since that minute contains data
        '{{ params.default_bin }}'::interval AS gap_adjustment
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
            --sum works because between 0 (synthetic) and 1 (real), we want 1 (implies real data)
            - SUM(gap_adjustment)
        AS bin_gap
    FROM fluffed_data
    GROUP BY
        sensor_id_col, 
        dt_col
),

--summarize gaps by sensor.
summarized_gaps AS (
    SELECT
        sensor_id_col,
        STRING_AGG(gap_start || '--' || gap_end, '; ') AS gaps
    FROM bin_times
    WHERE
        gap_end IS NOT NULL --NULL gap_end occurs at the end of the search period
        AND bin_gap >= '{{ params.gap_threshold }}'::interval
    GROUP BY sensor_id_col
)

SELECT
    'There were ' || COALESCE(COUNT(summarized_gaps.*), 0) || ' gaps larger than {{ params.gap_threshold }}.' AS summ, 
    array_agg(summarized_gaps) AS gaps
FROM summarized_gaps