--query to be used in `check_row_count` tasks to compare
--row count to average over lookback period.
--noqa: disable=TMP, PRS

WITH lookback AS ( --noqa: L045
    SELECT
        date_trunc('day', {{ params.dt_col }}) AS _dt, --noqa: L039
        COUNT(DISTINCT {{ params.id_col }}) AS count, --noqa: L039
        ARRAY_AGG(DISTINCT {{ params.id_col }}) AS daily_ids --noqa: L039
    FROM {{ params.table }}
    WHERE
        {{ params.dt_col }} >= '{{ ds }} 00:00:00'::timestamp - interval '{{ params.lookback }}'
        AND {{ params.dt_col }} < '{{ ds }} 00:00:00'::timestamp + interval '1 day'
    --group by day then avg excludes missing days.
    GROUP BY _dt --noqa: L003
),

ids_dif AS (
    SELECT ARRAY_AGG(ids_diff) AS ids_diff
    FROM (
        SELECT DISTINCT UNNEST(lb.daily_ids) AS ids_diff
        FROM lookback AS lb
        WHERE _dt != '{{ ds }}'::date
        EXCEPT
        SELECT UNNEST(today.daily_ids)
        FROM lookback AS today
        WHERE _dt = '{{ ds }}'::date
        ORDER BY ids_diff
    ) AS c
)

SELECT
    today.count >= FLOOR({{ params.threshold }}::numeric * AVG(lb.count)) AS check,
    'Daily count: ' || to_char(today.count, 'FM9,999,999,999')
    AS ds_count,
    initcap('{{ params.lookback }}') || ' Lookback Avg: '
        || to_char(AVG(lb.count), 'FM9,999,999,999')
    AS lookback_avg,
    'Pass threshold: ' || to_char(
        FLOOR({{ params.threshold }}::numeric * AVG(lb.count)),
        'FM9,999,999,999'
    )
    AS passing_value,
    CASE WHEN array_length(c.ids_diff, 1) > 0
        THEN 'The following {{ params.id_col }} were present within the last '
            || '{{ params.lookback }}, but absent today:' || c.ids_diff::text
        ELSE 'Sufficient {{ params.id_col }} present when compared to the last {{ params.lookback }}.'
    END AS id_diff
FROM lookback AS today,
    lookback AS lb, --noqa: L025
    ids_dif AS c
WHERE
    lb._dt != '{{ ds }}'::date
    AND today._dt = '{{ ds }}'::date
GROUP BY
    today.count,
    c.ids_diff