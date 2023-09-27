--query to be used in `check_row_count` tasks to compare
--row count to average over lookback period.
--noqa: disable=TMP, PRS

WITH lookback AS ( --noqa: L045
    SELECT
        date_trunc('day', {{ params.dt_col }}) AS _dt, --noqa: L039
        COUNT(*) AS lookback_count
    FROM {{ params.table }}
    WHERE
        {{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp - interval '{{ params.lookback }}'
        AND {{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp
        AND division_id = {{ params.div_id }}::int
    --group by day then avg excludes missing days.
    GROUP BY _dt --noqa: L003
)

SELECT
    COUNT(*) >= {{ params.threshold }}::numeric *
        (SELECT AVG(lookback_count) FROM lookback) AS check, 
    COUNT(*) AS ds_count,
    (SELECT AVG(lookback_count) FROM lookback) AS lookback_avg,
    {{ params.threshold }}::numeric *
        (SELECT AVG(lookback_count) FROM lookback) AS passing_value
FROM {{ params.table }}
WHERE
    {{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp
    AND {{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp + interval '1 day'
    AND division_id = {{ params.div_id }}::int