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
    --group by day then avg excludes missing days.
    GROUP BY _dt --noqa: L003
),

today AS (
    SELECT
        date_trunc('day', {{ params.dt_col }}) AS _dt, --noqa: L039
        SUM({{ params.col_to_sum }}) AS today_count
    FROM {{ params.table }}
    WHERE
        {{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp
        AND {{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp + interval '1 day'
    GROUP BY _dt --noqa: L003
)

SELECT
    a.today_count >= FLOOR({{ params.threshold }}::numeric * AVG(lb.lookback_count))
    AS check, 
    'Daily count: ' || to_char(
        a.today_count, 'FM9,999,999,999'
    ) AS ds_count,
    initcap('{{ params.lookback }}') || ' Lookback Avg: ' || to_char(
        AVG(lb.lookback_count), 'FM9,999,999,999'
    ) AS lookback_avg,
    'Pass threshold: ' || to_char(
        FLOOR({{ params.threshold }}::numeric * AVG(lb.lookback_count)),
        'FM9,999,999,999'
    ) AS passing_value
FROM today AS a
JOIN lookback AS lb ON
    NOT (date_part('isodow', a._dt) <= 5
        AND (
            SELECT hol.holiday FROM ref.holiday AS hol WHERE hol.dt = a._dt
        ) IS NULL)
    = NOT (
        date_part('isodow', lb._dt) <= 5
        AND (
            SELECT hol.holiday FROM ref.holiday AS hol WHERE hol.dt = lb._dt
        ) IS NULL)
GROUP BY a.today_count