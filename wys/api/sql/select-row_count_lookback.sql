--query to be used in `check_row_count` tasks to compare
--row count to average over lookback period.

WITH lookback AS (
    SELECT
        date_trunc('day', {{ params.dt_col }}) AS _dt,
        COUNT(*) AS lookback_count
    FROM {{ params.table }}
    WHERE
        {{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp - interval '{{ params.lookback }}'
        AND {{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp
    --group by day then avg excludes missing days.
    GROUP BY _dt
)

SELECT
    COUNT(*) >= {{ params.threshold }}::numeric * lba.lookback_avg AS check, 
    COUNT(*) AS ds_count,
    lba.lookback_avg,
    {{ params.threshold }}::numeric * lba.lookback_avg AS passing_value
FROM {{ params.table }},
LATERAL (
    SELECT AVG(lookback_count) AS lookback_avg FROM lookback
) AS lba
WHERE
    {{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp
    AND {{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp + interval '1 day'
GROUP BY lba.lookback_avg