--query to be used in `check_row_count` tasks to compare
--row count to average over lookback period.
-- noqa: disable=PRS, TMP

WITH lookback AS ( --noqa: L045
    SELECT
        date_trunc('day', {{ params.dt_col }}) AS _dt, --noqa: L039
        COUNT(DISTINCT {{ params.sensor_id_col }}) AS lookback_count
    FROM {{ params.table }}
    WHERE
        {{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp - interval '{{ params.lookback }}'
        AND {{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp
    --group by day then avg excludes missing days.
    GROUP BY _dt  --noqa: L003
)

SELECT
    COUNT(DISTINCT a.{{ params.sensor_id_col }}) >=
        FLOOR({{ params.threshold }}::numeric * lb.lookback_avg) AS check, --noqa: L026
    'Daily count: ' || to_char(
        COUNT(DISTINCT a.{{ params.sensor_id_col }}),
        'FM9,999,999,999'
        ) AS ds_count,
    initcap('{{ params.lookback }}') || ' Lookback Avg: '
        || to_char(lb.lookback_avg, 'FM9,999,999,999') AS lookback_avg,
    'Pass threshold: ' || to_char(
            FLOOR({{ params.threshold }}::numeric * lb.lookback_avg),
            'FM9,999,999,999'
            ) AS passing_value
FROM {{ params.table }} AS a,
LATERAL (
    SELECT AVG(lookback_count) AS lookback_avg FROM lookback
) AS lba
WHERE
    a.{{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp
    AND a.{{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp + interval '1 day'
GROUP BY lba.lookback_avg --noqa: L003, L026