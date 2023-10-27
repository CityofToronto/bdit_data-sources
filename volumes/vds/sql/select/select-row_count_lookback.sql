--query to be used in `check_row_count` tasks to compare
--row count to average over lookback period.
--noqa: disable=TMP, PRS

WITH lookback AS ( --noqa: L045
    SELECT
        date_trunc('day', {{ params.dt_col }}) AS _dt, --noqa: L039
        SUM({{ params.col_to_sum }}) AS lookback_count
    FROM {{ params.table }}
    WHERE
        {{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp - interval '{{ params.lookback }}'
        AND {{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp
        AND division_id = {{ params.div_id }}::int
    --group by day then avg excludes missing days.
    GROUP BY _dt --noqa: L003
)

SELECT
    SUM({{ params.col_to_sum }}) >= FLOOR({{ params.threshold }}::numeric * lb.lookback_avg) AS check, 
    'Daily count: ' || to_char(SUM({{ params.col_to_sum }}), 'FM9,999,999,999') AS ds_count,
    initcap('{{ params.lookback }}') || ' Lookback Avg: '
        || to_char(lb.lookback_avg, 'FM9,999,999,999') AS lookback_avg,
    'Pass threshold: ' || to_char(
            FLOOR({{ params.threshold }}::numeric * lb.lookback_avg),
            'FM9,999,999,999'
            ) AS passing_value
FROM {{ params.table }} AS a,
    LATERAL (
        SELECT AVG(lookback_count) lookback_avg FROM lookback
    ) AS lb
WHERE
    a.{{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp
    AND a.{{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp + interval '1 day'
    AND a.division_id = {{ params.div_id }}::int
GROUP BY lb.lookback_avg